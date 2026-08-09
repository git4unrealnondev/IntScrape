use anyhow::Result;
use arti_client::{TorClient, TorClientConfig};
use axum::extract::{FromRef, Path, Request};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use futures::StreamExt;
use hyper::StatusCode;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server;
use safelog::{DisplayRedacted as _, sensitive};
use shared_types::{GlobalCallbacks, Tag};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::sync::watch;
use tor_cell::relaycell::msg::Connected;
use tor_hsservice::StreamRequest;
use tor_hsservice::config::OnionServiceConfigBuilder;
use tor_proto::stream::IncomingStreamRequest;
use tower::Service;
use tower::ServiceExt;
use tower_http::services::ServeFile;

const API_VERSION: u64 = 1;

#[unsafe(no_mangle)]
fn on_start() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            main().await;
        })
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![shared_types::Plugin {
        name: "tor-serve".into(),
        callbacks: vec![GlobalCallbacks::Start(
            shared_types::StartupThreadType::Spawn,
        )],
        ..Default::default()
    }]
}

/// Gets tags associated with a file
pub async fn get_file_tags(Path(file_id): Path<u64>) -> Json<HashSet<Tag>> {
    let tag_ids = client::relationship_get_tagid(file_id).unwrap_or_default();
    let tags = client::get_tag_id_bulk(tag_ids)
        .unwrap_or_default()
        .into_values()
        .collect();

    Json(tags)
}

/// Gets tag ids associated with a file
pub async fn get_file_tag_ids(Path(file_id): Path<u64>) -> Json<HashSet<u64>> {
    let tag_ids = client::relationship_get_tagid(file_id).unwrap_or_default();

    Json(tag_ids)
}

/// Gets a file directly from disk
pub async fn get_file(Path(file_id): Path<u64>, req: Request) -> impl IntoResponse {
    if let Some(filepath) = client::get_file_path(file_id).unwrap() {
        let path = PathBuf::from(&filepath);

        // Initialize the ServeFile service
        let service = ServeFile::new(path);

        // Run the service with the incoming request using oneshot
        match service.oneshot(req).await {
            Ok(response) => return response.into_response(),
            Err(_) => {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn main() {
    tracing_subscriber::fmt::init();

    let router = Router::new()
        .route("/api/v1/file/{file_id}", get(get_file))
        .route("/api/v1/file/{file_id}/tag_ids", get(get_file_tag_ids))
        .route("/api/v1/file/{file_id}/tags", get(get_file_tags))
        .route(
            "/",
            get(|| async { format!("Hello from version: {}", API_VERSION) }),
        );

    let config = TorClientConfig::default();

    let client = TorClient::create_bootstrapped(config).await.unwrap();

    let svc_cfg = OnionServiceConfigBuilder::default()
        .nickname("intscrape-server".parse().unwrap())
        .build()
        .unwrap();

    let Some((service, request_stream)) = client.launch_onion_service(svc_cfg).unwrap() else {
        let _ = client::log_silent(format!("onion service was disabled in config"));
        return;
    };

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        loop {
            let should_exit = match client::should_exit() {
                Ok(value) => value,
                Err(error) => {
                    let _ = client::log_silent(format!("could not check exit status: {error}"));
                    true // Treat an error as a shutdown request, if appropriate.
                }
            };

            if should_exit {
                let _ = shutdown_tx.send(true);
                break;
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    let mut status_events = service.status_events();

    loop {
        tokio::select! {
            status = status_events.next() => {
                match status {
                    Some(status) => {
                  let _ =      client::log_silent(format!("onion service status: {:?}", status.state()));

                        if status.state().is_fully_reachable() {
                            break;
                        }
                    }

                    None => {
                     let _ =   client::log_silent(format!("onion service status stream ended"));
                        return;
                    }
                }
            }

            result = shutdown_rx.changed() => {
                match result {
                    Ok(()) if *shutdown_rx.borrow() => {
                    let _ =    client::log_silent(format!("shutting down before service became reachable"));
                        drop(service);
                        return;
                    }

                    Ok(()) => {}

                    Err(_) => {
                  let _ =      client::log_silent(format!("shutdown sender was dropped"));
                        drop(service);
                        return;
                    }
                }
            }
        }
    }

    let _ = client::log_silent(format!(
        "ready to serve connections on: {}",
        service.onion_address().unwrap().display_unredacted()
    ));
    let stream_requests = tor_hsservice::handle_rend_requests(request_stream);
    tokio::pin!(stream_requests);

    loop {
        tokio::select! {
            request = stream_requests.next() => {
                let Some(stream_request) = request else {
                    break;
                };

                let router = router.clone();

                tokio::spawn(async move {
                    let request = stream_request.request().clone();

                    if let Err(err) =
                        handle_stream_request(stream_request, router).await
                    {
                     let _ =   client::log_silent(format!(
                            "error serving connection {:?}: {}",
                            sensitive(request),
                            err
                        ));
                    }
                });
            }

            result = shutdown_rx.changed() => {
                if result.is_ok() && *shutdown_rx.borrow() {
                  let _ =  client::log_silent(format!("shutting down onion service"));
                    break;
                }
            }
        }
    }

    // Dropping the service and request stream stops accepting new requests.
    drop(service);
    let _ = client::log_silent(format!("onion service exited cleanly"));
}

async fn handle_stream_request(stream_request: StreamRequest, router: Router) -> Result<()> {
    match stream_request.request() {
        IncomingStreamRequest::Begin(begin) if begin.port() == 80 => {
            let onion_service_stream = stream_request.accept(Connected::new_empty()).await?;
            let io = TokioIo::new(onion_service_stream);

            let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| {
                router.clone().call(request)
            });

            server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_service)
                .await
                .map_err(|x| anyhow::anyhow!(x))?;
        }
        _ => {
            stream_request.shutdown_circuit()?;
        }
    }

    Ok(())
}

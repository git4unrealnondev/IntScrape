use std::fmt;

use shared_types::{PluginProperties, ScraperDataReturn};

pub enum Site {
    E6,
    E6AI,
}

impl fmt::Display for Site {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Site::E6 => "E621",
            Site::E6AI => "E6AI",
        };
        write!(f, "{s}")
    }
}

#[unsafe(no_mangle)]
fn get_plugin_info() -> Vec<shared_types::Plugin> {
    vec![
        shared_types::Plugin {
            name: "E621".into(),
            properties: vec![
                PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
                PluginProperties::Sites(vec!["e6".into(), "E621".into(), "e621.com".into()]),
            ],
            ..Default::default()
        },
        shared_types::Plugin {
            name: "E6AI".into(),
            properties: vec![
                PluginProperties::Ratelimit(1, std::time::Duration::from_secs(1)),
                PluginProperties::Sites(vec!["e6ai".into(), "E6AI".into(), "e6ai.com".into()]),
            ],
            ..Default::default()
        },
    ]
}

#[unsafe(no_mangle)]
pub fn url_dump(
    scraperdata: &shared_types::ScraperDataReturn,
) -> Vec<shared_types::ScraperDataReturn> {
    let mut out = Vec::new();

    let site = if ["e6", "e621", "e621.com"].contains(&scraperdata.job.site.to_lowercase().as_str())
    {
        Site::E6
    } else {
        Site::E6AI
    };

    let hardlimit = 751;

    for i in 1..hardlimit {
        let url = build_url(&scraperdata.job.param, i, &site);

        out.push(ScraperDataReturn {
            job: shared_types::PluginJob {
                site: scraperdata.job.site.clone(),
                priority: shared_types::DEFAULT_PRIORITY - 2,
                param: vec![shared_types::ScraperParam::Url(url)],
                ..Default::default()
            },
            ..Default::default()
        })
    }

    out
}

fn build_url(params: &[shared_types::ScraperParam], pagenum: u64, site: &Site) -> String {
    if params.is_empty() {
        return String::new();
    }

    let lowercase_site = site.to_string().to_lowercase();
    let mut url = format!("https://{}.net/posts.json", lowercase_site);

    let login_info = params.iter().find_map(|p| {
        if let shared_types::ScraperParam::Login(shared_types::LoginType::ApiNamespaced(
            _,
            Some(user),
            Some(key),
        )) = p
        {
            Some((user, key))
        } else {
            None
        }
    });

    let tags: Vec<&str> = params
        .iter()
        .filter_map(|p| {
            if let shared_types::ScraperParam::Normal(tag) = p {
                Some(tag.as_str())
            } else {
                None
            }
        })
        .collect();

    if tags.is_empty() {
        return String::new();
    }

    if let Some((username, api_key)) = login_info {
        url += &format!("?login={}&api_key={}", username, api_key);
        url += &format!("&tags={}", tags.join("+"));
    } else {
        url += &format!("?tags={}", tags.join("+"));
    }

    // 4. Append the pagination tracker at the tail end
    url += &format!("&page={}", pagenum);

    url
}

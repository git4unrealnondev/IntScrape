use shared_types::{GenericNamespaceObj, Tag, TagSearch};
use std::cmp::Ordering;
use std::collections::HashMap;
use strsim::levenshtein;

const HIGH_VALUE_TAG_COUNT: u64 = 5;

#[derive(Clone)]
pub(crate) struct TagEntry {
    pub(crate) tag_id: u64,
    pub(crate) name: String,
    normalized_name: String,
    pub(crate) count: u64,
    pub(crate) tag: Tag,
}

#[derive(Default)]
pub(crate) struct TagSearchCache {
    entries: Vec<TagEntry>,
    exact: HashMap<String, Vec<usize>>,
}

impl TagSearchCache {
    pub(crate) fn from_entries(entries: Vec<TagEntry>) -> Self {
        let mut exact: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, entry) in entries.iter().enumerate() {
            exact
                .entry(entry.normalized_name.clone())
                .or_default()
                .push(index);
        }
        Self { entries, exact }
    }

    pub(crate) fn search(&self, query: &str, limit: usize) -> Vec<TagSearch> {
        let normalized_query = normalize(query);
        if normalized_query.is_empty() || limit == 0 {
            return Vec::new();
        }

        let mut matches = self
            .exact
            .get(&normalized_query)
            .into_iter()
            .flatten()
            .map(|index| (&self.entries[*index], 0usize))
            .collect::<Vec<_>>();

        for entry in &self.entries {
            if matches
                .iter()
                .any(|(matched, _)| matched.tag_id == entry.tag_id)
            {
                continue;
            }
            if let Some(score) = match_score(&normalized_query, &entry.normalized_name) {
                matches.push((entry, score));
            }
        }

        matches.sort_unstable_by(|(left, left_distance), (right, right_distance)| {
            left_distance
                .cmp(right_distance)
                .then_with(|| right.count.cmp(&left.count))
                .then_with(|| left.name.cmp(&right.name))
        });
        matches
            .into_iter()
            .take(limit)
            .map(|(entry, _)| TagSearch {
                tag: entry.tag.clone(),
                tag_id: entry.tag_id,
                count: entry.count,
            })
            .collect()
    }
}

pub(crate) fn tag_entry(
    tag_id: u64,
    name: String,
    count: u64,
    namespace: String,
    description: Option<String>,
) -> TagEntry {
    TagEntry {
        tag_id,
        normalized_name: normalize(&name),
        name: name.clone(),
        count,
        tag: Tag {
            name,
            namespace: GenericNamespaceObj {
                name: namespace,
                description,
            },
        },
    }
}

pub(crate) fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn allowed_distance(length: usize) -> usize {
    match length {
        0..=4 => 1,
        5..=8 => 2,
        _ => 3,
    }
}

fn fuzzy_distance(left: &str, right: &str) -> usize {
    levenshtein(left, right)
}

fn match_score(query: &str, candidate: &str) -> Option<usize> {
    if candidate.starts_with(query) {
        return Some(0);
    }
    if candidate.contains(query) {
        return Some(1);
    }

    let distance = fuzzy_distance(query, candidate);
    (distance <= allowed_distance(query.len())).then_some(distance + 2)
}

pub(crate) fn compare_results(left: &TagSearch, right: &TagSearch) -> Ordering {
    right
        .count
        .cmp(&left.count)
        .then_with(|| left.tag.name.cmp(&right.tag.name))
        .then_with(|| left.tag_id.cmp(&right.tag_id))
}

pub(crate) const fn high_value_count() -> u64 {
    HIGH_VALUE_TAG_COUNT
}

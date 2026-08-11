use shared_types::TagSearch;
use std::cmp::Ordering;
use std::collections::HashSet;
use strsim::levenshtein;

const HIGH_VALUE_TAG_COUNT: u64 = 5;

pub(crate) struct TagEntry {
    pub(crate) tag_id: u64,
    normalized_name: String,
    pub(crate) count: u64,
}

struct CompactTagEntry {
    tag_id: u64,
    name_start: u32,
    name_len: u32,
    count: u64,
}

#[derive(Default)]
pub(crate) struct TagSearchCache {
    entries: Vec<CompactTagEntry>,
    names: Vec<u8>,
}

impl TagSearchCache {
    pub(crate) fn from_entries(entries: Vec<TagEntry>) -> Self {
        let mut compact_entries = Vec::with_capacity(entries.len());
        let mut names = Vec::new();
        for entry in entries {
            let name_start = names.len() as u32;
            names.extend_from_slice(entry.normalized_name.as_bytes());
            compact_entries.push(CompactTagEntry {
                tag_id: entry.tag_id,
                name_start,
                name_len: entry.normalized_name.len() as u32,
                count: entry.count,
            });
        }
        Self {
            entries: compact_entries,
            names,
        }
    }

    pub(crate) fn search(&self, query: &str, limit: usize) -> Vec<TagSearch> {
        let normalized_query = normalize(query);
        if normalized_query.is_empty() || limit == 0 {
            return Vec::new();
        }

        let mut matches = Vec::with_capacity(limit);
        for entry in &self.entries {
            let name = self.entry_name(entry);
            if let Some(score) = match_score(&normalized_query, name) {
                if matches.len() < limit {
                    matches.push((entry, score));
                } else {
                    let mut worst_index = 0;
                    for index in 1..matches.len() {
                        if is_worse(matches[index], matches[worst_index]) {
                            worst_index = index;
                        }
                    }
                    if is_better(score, entry, matches[worst_index]) {
                        matches[worst_index] = (entry, score);
                    }
                }
            }
        }

        matches.sort_unstable_by(|(left, left_distance), (right, right_distance)| {
            left_distance
                .cmp(right_distance)
                .then_with(|| right.count.cmp(&left.count))
                .then_with(|| left.tag_id.cmp(&right.tag_id))
        });
        matches
            .into_iter()
            .take(limit)
            .map(|(entry, _)| TagSearch {
                tag_id: entry.tag_id,
                count: entry.count,
            })
            .collect()
    }

    fn entry_name<'a>(&'a self, entry: &CompactTagEntry) -> &'a str {
        std::str::from_utf8(
            &self.names[entry.name_start as usize..(entry.name_start + entry.name_len) as usize],
        )
        .expect("normalized tag names are valid UTF-8")
    }
}

fn is_worse(left: (&CompactTagEntry, usize), right: (&CompactTagEntry, usize)) -> bool {
    left.1 > right.1
        || (left.1 == right.1
            && (left.0.count < right.0.count
                || (left.0.count == right.0.count && left.0.tag_id > right.0.tag_id)))
}

fn is_better(score: usize, entry: &CompactTagEntry, worst: (&CompactTagEntry, usize)) -> bool {
    score < worst.1
        || (score == worst.1
            && (entry.count > worst.0.count
                || (entry.count == worst.0.count && entry.tag_id < worst.0.tag_id)))
}

/// Searches a streamed set of entries without materializing the full set.
pub(crate) fn search_entries<I>(
    entries: I,
    query: &str,
    limit: usize,
    excluded_ids: &HashSet<u64>,
) -> Vec<TagSearch>
where
    I: IntoIterator<Item = TagEntry>,
{
    let normalized_query = normalize(query);
    if normalized_query.is_empty() || limit == 0 {
        return Vec::new();
    }

    let mut matches: Vec<(TagEntry, usize)> = Vec::with_capacity(limit);
    for entry in entries {
        if excluded_ids.contains(&entry.tag_id) {
            continue;
        }

        let Some(score) = match_score(&normalized_query, &entry.normalized_name) else {
            continue;
        };

        if matches.len() < limit {
            matches.push((entry, score));
            continue;
        }

        let mut worst_index = 0;
        for index in 1..matches.len() {
            let (_, candidate_score) = &matches[index];
            let (_, worst_score) = &matches[worst_index];
            if candidate_score > worst_score
                || (candidate_score == worst_score
                    && (matches[index].0.count < matches[worst_index].0.count
                        || (matches[index].0.count == matches[worst_index].0.count
                            && matches[index].0.tag_id > matches[worst_index].0.tag_id)))
            {
                worst_index = index;
            }
        }

        let (_, worst_score) = &matches[worst_index];
        let is_better = score < *worst_score
            || (score == *worst_score
                && (entry.count > matches[worst_index].0.count
                    || (entry.count == matches[worst_index].0.count
                        && entry.tag_id < matches[worst_index].0.tag_id)));
        if is_better {
            matches[worst_index] = (entry, score);
        }
    }

    matches.sort_unstable_by(|(left, left_score), (right, right_score)| {
        left_score
            .cmp(right_score)
            .then_with(|| right.count.cmp(&left.count))
            .then_with(|| left.tag_id.cmp(&right.tag_id))
    });
    matches
        .into_iter()
        .map(|(entry, _)| TagSearch {
            tag_id: entry.tag_id,
            count: entry.count,
        })
        .collect()
}

pub(crate) fn tag_entry(tag_id: u64, name: &str, count: u64) -> TagEntry {
    TagEntry {
        tag_id,
        normalized_name: normalize(name),
        count,
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
        .then_with(|| left.tag_id.cmp(&right.tag_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_search_keeps_only_requested_result_count() {
        let entries = (0..10_000).map(|id| tag_entry(id, &format!("common-tag-{id}"), id));

        let results = search_entries(entries, "common", 3, &HashSet::new());

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].tag_id, 9999);
    }
}

pub(crate) const fn high_value_count() -> u64 {
    HIGH_VALUE_TAG_COUNT
}

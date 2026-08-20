use shared_types::TagSearch;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

pub(crate) const FTS_CANDIDATE_LIMIT: usize = 4096;

pub(crate) struct TagEntry {
    pub(crate) tag_id: u64,
    normalized_name: String,
    pub(crate) count: u64,
}

struct SearchMatch<T> {
    entry: T,
    score: usize,
    count: u64,
    tag_id: u64,
}

impl<T> PartialEq for SearchMatch<T> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.count == other.count && self.tag_id == other.tag_id
    }
}

impl<T> Eq for SearchMatch<T> {}

impl<T> PartialOrd for SearchMatch<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for SearchMatch<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| other.count.cmp(&self.count))
            .then_with(|| self.tag_id.cmp(&other.tag_id))
    }
}

fn is_better<T>(score: usize, count: u64, tag_id: u64, worst: &SearchMatch<T>) -> bool {
    score < worst.score
        || (score == worst.score
            && (count > worst.count || (count == worst.count && tag_id < worst.tag_id)))
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

    let mut matches = BinaryHeap::with_capacity(limit);
    for entry in entries {
        if excluded_ids.contains(&entry.tag_id) {
            continue;
        }

        let Some(score) = match_score(&normalized_query, &entry.normalized_name) else {
            continue;
        };

        if matches.len() < limit {
            let count = entry.count;
            let tag_id = entry.tag_id;
            matches.push(SearchMatch {
                entry,
                score,
                count,
                tag_id,
            });
            continue;
        }

        if is_better(score, entry.count, entry.tag_id, matches.peek().unwrap()) {
            matches.pop();
            let count = entry.count;
            let tag_id = entry.tag_id;
            matches.push(SearchMatch {
                entry,
                score,
                count,
                tag_id,
            });
        }
    }

    let mut matches = matches.into_vec();
    matches.sort_unstable_by(|left, right| left.cmp(right));
    matches
        .into_iter()
        .map(|candidate| TagSearch {
            tag_id: candidate.entry.tag_id,
            count: candidate.entry.count,
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

/// Builds a safe FTS5 query from the user's words. Individual words preserve
/// substring matching while OR keeps typo candidates available for ranking.
pub(crate) fn fts_query(value: &str) -> Option<String> {
    let normalized = normalize(value);
    let mut terms = Vec::new();
    if normalized.chars().count() >= 3 {
        terms.push(format!("\"{normalized}\""));
    }
    terms.extend(
        value
            .split(|character: char| !character.is_alphanumeric())
            .map(normalize)
            .filter(|term| term.chars().count() >= 3)
            .flat_map(|term| {
                let mut queries = vec![format!("\"{}\"", term.replace('"', ""))];
                let characters: Vec<char> = term.chars().collect();
                for window in characters.windows(3) {
                    queries.push(window.iter().collect());
                    queries.push(format!("{}{}{}", window[1], window[0], window[2]));
                }
                for index in 0..characters.len().saturating_sub(1) {
                    let mut swapped = characters.clone();
                    swapped.swap(index, index + 1);
                    queries.push(swapped.into_iter().collect());
                }
                queries
            }),
    );
    (!terms.is_empty()).then(|| terms.join(" OR "))
}

fn allowed_distance(length: usize) -> usize {
    match length {
        0..=4 => 1,
        5..=8 => 2,
        _ => 3,
    }
}

fn fuzzy_distance(left: &str, right: &str) -> usize {
    let cutoff = allowed_distance(left.chars().count());
    if left.chars().count().abs_diff(right.chars().count()) > cutoff {
        return usize::MAX;
    }

    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();
    let unreachable = cutoff + 1;
    let mut previous = vec![unreachable; right.len() + 1];
    let mut current = vec![unreachable; right.len() + 1];
    for index in 0..=right.len().min(cutoff) {
        previous[index] = index;
    }

    for (left_index, left_char) in left.iter().enumerate() {
        current.fill(unreachable);
        let start = left_index.saturating_sub(cutoff);
        let end = (left_index + cutoff + 1).min(right.len());
        if start == 0 {
            current[0] = left_index + 1;
        }
        for right_index in start..end {
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + usize::from(left_char != &right[right_index]));
        }
        std::mem::swap(&mut previous, &mut current);
    }

    previous[right.len()]
}

fn match_score(query: &str, candidate: &str) -> Option<usize> {
    if candidate.starts_with(query) {
        return Some(0);
    }
    if candidate.contains(query) {
        return Some(1);
    }

    let distance = fuzzy_distance(query, candidate);
    (distance != usize::MAX).then_some(distance + 2)
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

    #[test]
    fn fuzzy_distance_returns_cutoff_matches_without_full_matrix() {
        assert_eq!(fuzzy_distance("raer", "rare"), 2);
        assert_eq!(fuzzy_distance("female", "females"), 1);
        assert_eq!(fuzzy_distance("cat", "long-unrelated-tag"), usize::MAX);
    }

    #[test]
    fn fts_query_includes_substring_and_transposition_candidates() {
        let query = fts_query("raer creatur").unwrap();
        assert!(query.contains("raer"));
        assert!(query.contains("rare"));
        assert!(query.contains("creatur"));
    }
}

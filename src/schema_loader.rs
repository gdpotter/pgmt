use anyhow::{Context, Result, anyhow};
use petgraph::algo::tarjan_scc;
use petgraph::graph::DiGraph;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

/// Represents a schema file with its content and dependencies
#[derive(Debug, Clone)]
pub struct SchemaFile {
    pub relative_path: String,
    pub content: String,
    pub dependencies: Vec<String>,
}

/// Configuration for schema loading
#[derive(Debug, Clone)]
pub struct SchemaLoaderConfig {
    pub schema_dir: PathBuf,
}

impl SchemaLoaderConfig {
    pub fn new(schema_dir: PathBuf) -> Self {
        Self { schema_dir }
    }
}

/// Handles discovery, parsing, and ordering of schema files
pub struct SchemaLoader {
    config: SchemaLoaderConfig,
}

impl SchemaLoader {
    pub fn new(config: SchemaLoaderConfig) -> Self {
        Self { config }
    }

    /// Load and order schema files, returning the individual files
    pub fn load_ordered_schema_files(&self) -> Result<Vec<SchemaFile>> {
        let files = self.discover_schema_files()?;
        let parsed_files = self.parse_schema_files(files)?;
        let ordered_files = self.resolve_dependencies(parsed_files)?;

        Ok(ordered_files)
    }

    /// Discover all .sql files in the schema directory recursively
    fn discover_schema_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        self.discover_sql_files_recursive(&self.config.schema_dir, &mut files)?;

        // Sort alphabetically for deterministic ordering
        files.sort();

        Ok(files)
    }

    /// Recursively discover .sql files in a directory
    fn discover_sql_files_recursive(&self, dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }

        let entries = fs::read_dir(dir)
            .with_context(|| format!("Failed to read directory: {}", dir.display()))?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            // Security check: ensure symlinks don't point outside schema directory
            if path.is_symlink() {
                let real_path = fs::canonicalize(&path)
                    .with_context(|| format!("Failed to resolve symlink: {}", path.display()))?;
                let schema_dir_canonical =
                    fs::canonicalize(&self.config.schema_dir).with_context(|| {
                        format!(
                            "Failed to resolve schema directory: {}",
                            self.config.schema_dir.display()
                        )
                    })?;

                if !real_path.starts_with(&schema_dir_canonical) {
                    return Err(anyhow!(
                        "Symlink points outside schema directory: {} -> {}",
                        path.display(),
                        real_path.display()
                    ));
                }
            }

            if path.is_dir() {
                self.discover_sql_files_recursive(&path, files)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                files.push(path);
            }
        }

        Ok(())
    }

    /// Parse schema files and extract their dependencies
    fn parse_schema_files(&self, file_paths: Vec<PathBuf>) -> Result<Vec<SchemaFile>> {
        let mut schema_files = Vec::new();

        for path in file_paths {
            let content = fs::read_to_string(&path)
                .with_context(|| format!("Failed to read file: {}", path.display()))?;

            let relative_path = path
                .strip_prefix(&self.config.schema_dir)
                .map_err(|_| anyhow!("File path not within schema directory: {}", path.display()))?
                .to_string_lossy()
                .to_string();

            let dependencies = self.parse_dependencies(&content)?;

            schema_files.push(SchemaFile {
                relative_path,
                content,
                dependencies,
            });
        }

        Ok(schema_files)
    }

    /// Parse dependency declarations from SQL file content
    fn parse_dependencies(&self, content: &str) -> Result<Vec<String>> {
        let mut dependencies = Vec::new();

        for line in content.lines() {
            let trimmed = line.trim();

            // Look for -- require: syntax
            if let Some(require_part) = trimmed.strip_prefix("-- require:") {
                let require_content = require_part.trim();

                // Handle multiple dependencies separated by commas
                for dep in require_content.split(',') {
                    let dep = dep.trim();
                    if !dep.is_empty() {
                        // Normalize the dependency path
                        let normalized = self.normalize_dependency_path(dep)?;
                        dependencies.push(normalized);
                    }
                }
            }
        }

        Ok(dependencies)
    }

    /// Normalize a dependency path (add .sql extension if missing, resolve relative paths)
    fn normalize_dependency_path(&self, dep_path: &str) -> Result<String> {
        let mut path = dep_path.to_string();

        // Add .sql extension if not present
        if !path.ends_with(".sql") {
            path.push_str(".sql");
        }

        // For now, all paths are relative to schema root
        // TODO: In the future, we could support ./ and ../ relative paths

        Ok(path)
    }

    /// Resolve dependencies and return files in correct order
    fn resolve_dependencies(&self, files: Vec<SchemaFile>) -> Result<Vec<SchemaFile>> {
        // Create a map of relative path -> file for quick lookup
        let file_map: HashMap<String, SchemaFile> = files
            .iter()
            .map(|f| (f.relative_path.clone(), f.clone()))
            .collect();

        // Validate all dependencies exist
        for file in &files {
            for dep in &file.dependencies {
                if !file_map.contains_key(dep) {
                    return Err(anyhow!(
                        "Missing dependency '{}' required by '{}'",
                        dep,
                        file.relative_path
                    ));
                }
            }
        }

        // Topological sort using Kahn's algorithm
        self.topological_sort(files, file_map)
    }

    /// Perform topological sort to order files by dependencies
    fn topological_sort(
        &self,
        files: Vec<SchemaFile>,
        file_map: HashMap<String, SchemaFile>,
    ) -> Result<Vec<SchemaFile>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();

        // Initialize in-degree counts and adjacency list
        for file in &files {
            in_degree.insert(file.relative_path.clone(), 0);
            graph.insert(file.relative_path.clone(), Vec::new());
        }

        // Build the dependency graph
        for file in &files {
            for dep in &file.dependencies {
                graph.get_mut(dep).unwrap().push(file.relative_path.clone());
                *in_degree.get_mut(&file.relative_path).unwrap() += 1;
            }
        }

        // Start with files that have no dependencies
        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|(_, count)| **count == 0)
            .map(|(path, _)| path.clone())
            .collect();

        // For deterministic ordering, sort the initial queue
        let mut queue_vec: Vec<_> = queue.into_iter().collect();
        queue_vec.sort();
        queue = queue_vec.into();

        let mut ordered = Vec::new();
        let mut processed = HashSet::new();

        while let Some(current) = queue.pop_front() {
            if processed.contains(&current) {
                continue;
            }

            ordered.push(file_map.get(&current).unwrap().clone());
            processed.insert(current.clone());

            // Process dependencies of current file
            let dependents = graph.get(&current).unwrap().clone();
            for dependent in dependents {
                let count = in_degree.get_mut(&dependent).unwrap();
                *count -= 1;

                if *count == 0 {
                    queue.push_back(dependent);
                }
            }

            // Sort queue for deterministic ordering
            let mut queue_vec: Vec<_> = queue.into_iter().collect();
            queue_vec.sort();
            queue = queue_vec.into();
        }

        // Check for circular dependencies
        if ordered.len() != files.len() {
            let unprocessed: HashSet<_> = files
                .iter()
                .filter(|f| !processed.contains(&f.relative_path))
                .map(|f| f.relative_path.clone())
                .collect();

            // Build a map of file -> dependencies for cycle detection
            let file_dependencies: HashMap<_, _> = files
                .iter()
                .map(|f| (f.relative_path.clone(), f.dependencies.clone()))
                .collect();

            let cycles = Self::find_cycles(&unprocessed, &file_dependencies);

            if cycles.is_empty() {
                // Fallback (shouldn't happen if our algorithm is correct)
                let mut unprocessed_list: Vec<_> = unprocessed.into_iter().collect();
                unprocessed_list.sort();
                return Err(anyhow!(
                    "Circular dependency detected. Files involved: {}",
                    unprocessed_list.join(", ")
                ));
            }

            // Format error message with actual cycles
            let cycle_descriptions: Vec<String> = cycles
                .iter()
                .map(|c| Self::format_cycle(c, &file_dependencies))
                .collect();

            if cycles.len() == 1 {
                return Err(anyhow!(
                    "Circular dependency detected: {}",
                    cycle_descriptions[0]
                ));
            } else {
                return Err(anyhow!(
                    "Multiple circular dependencies detected:\n  - {}",
                    cycle_descriptions.join("\n  - ")
                ));
            }
        }

        Ok(ordered)
    }

    /// Find actual cycles in a dependency graph using Tarjan's SCC algorithm.
    /// Returns a list of cycles, where each cycle is a vector of file paths.
    fn find_cycles(
        unprocessed: &HashSet<String>,
        file_dependencies: &HashMap<String, Vec<String>>,
    ) -> Vec<Vec<String>> {
        // Build a petgraph from unprocessed nodes only
        let mut pg: DiGraph<String, ()> = DiGraph::new();
        let mut node_indices: BTreeMap<String, _> = BTreeMap::new();

        // Add nodes (using BTreeMap for deterministic ordering)
        let mut sorted_unprocessed: Vec<_> = unprocessed.iter().cloned().collect();
        sorted_unprocessed.sort();

        for path in &sorted_unprocessed {
            let idx = pg.add_node(path.clone());
            node_indices.insert(path.clone(), idx);
        }

        // Add edges: dependency -> file (dependency must come before file)
        // This is the reverse of the "file depends on dependency" relationship
        for path in &sorted_unprocessed {
            if let Some(deps) = file_dependencies.get(path) {
                for dep in deps {
                    if unprocessed.contains(dep)
                        && let (Some(&from), Some(&to)) =
                            (node_indices.get(dep), node_indices.get(path))
                    {
                        pg.add_edge(from, to, ());
                    }
                }
            }
        }

        // Find SCCs - components with >1 node are cycles
        let sccs = tarjan_scc(&pg);

        sccs.into_iter()
            .filter(|scc| scc.len() > 1)
            .map(|scc| {
                let mut cycle: Vec<_> = scc.into_iter().map(|idx| pg[idx].clone()).collect();
                cycle.sort(); // Deterministic ordering
                cycle
            })
            .collect()
    }

    /// Format a cycle as a readable string showing the dependency chain.
    fn format_cycle(
        cycle_nodes: &[String],
        file_dependencies: &HashMap<String, Vec<String>>,
    ) -> String {
        if cycle_nodes.is_empty() {
            return String::new();
        }

        // Start from the first node alphabetically for determinism
        let mut sorted_nodes = cycle_nodes.to_vec();
        sorted_nodes.sort();

        let start = &sorted_nodes[0];
        let mut path = vec![start.clone()];
        let mut current = start.clone();
        let mut visited = HashSet::new();
        visited.insert(current.clone());

        // Follow dependencies to build the cycle path
        // We need to find which node in the cycle depends on the current node
        while path.len() < cycle_nodes.len() {
            let mut found = false;
            // Find a node in the cycle that depends on the current node
            for node in &sorted_nodes {
                if visited.contains(node) {
                    continue;
                }
                if let Some(deps) = file_dependencies.get(node)
                    && deps.contains(&current)
                {
                    path.push(node.clone());
                    visited.insert(node.clone());
                    current = node.clone();
                    found = true;
                    break;
                }
            }
            if !found {
                break;
            }
        }

        // Close the cycle by adding the start node again
        path.push(start.clone());

        path.join(" -> ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_schema_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn write_file(dir: &Path, relative_path: &str, content: &str) {
        let file_path = dir.join(relative_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(file_path, content).unwrap();
    }

    /// Helper to find a file's position in the ordered list
    fn find_file_index(files: &[SchemaFile], name: &str) -> usize {
        files
            .iter()
            .position(|f| f.relative_path.contains(name))
            .unwrap_or_else(|| panic!("File {} not found", name))
    }

    #[test]
    fn test_single_file() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        write_file(schema_dir, "my_schema.sql", "CREATE TABLE test (id INT);");

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let files = loader.load_ordered_schema_files().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].relative_path, "my_schema.sql");
        assert!(files[0].content.contains("CREATE TABLE test"));
    }

    #[test]
    fn test_multi_file_without_dependencies() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        write_file(
            schema_dir,
            "tables/users.sql",
            "CREATE TABLE users (id INT);",
        );
        write_file(
            schema_dir,
            "tables/posts.sql",
            "CREATE TABLE posts (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let files = loader.load_ordered_schema_files().unwrap();

        // Should be alphabetically ordered (posts before users)
        assert_eq!(files.len(), 2);
        let posts_idx = find_file_index(&files, "posts.sql");
        let users_idx = find_file_index(&files, "users.sql");
        assert!(posts_idx < users_idx);
    }

    #[test]
    fn test_dependency_parsing() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        write_file(schema_dir, "base.sql", "CREATE SCHEMA app;");
        write_file(
            schema_dir,
            "tables.sql",
            "-- require: base.sql\nCREATE TABLE app.users (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let files = loader.load_ordered_schema_files().unwrap();

        // base.sql should come before tables.sql
        let base_idx = find_file_index(&files, "base.sql");
        let tables_idx = find_file_index(&files, "tables.sql");
        assert!(base_idx < tables_idx);
    }

    #[test]
    fn test_multiple_dependencies() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        write_file(schema_dir, "schema.sql", "CREATE SCHEMA app;");
        write_file(
            schema_dir,
            "types.sql",
            "-- require: schema.sql\nCREATE TYPE app.status AS ENUM ('active', 'inactive');",
        );
        write_file(
            schema_dir,
            "tables.sql",
            "-- require: schema.sql, types.sql\nCREATE TABLE app.users (id INT, status app.status);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let files = loader.load_ordered_schema_files().unwrap();

        let schema_idx = find_file_index(&files, "schema.sql");
        let types_idx = find_file_index(&files, "types.sql");
        let tables_idx = find_file_index(&files, "tables.sql");

        assert!(schema_idx < types_idx);
        assert!(schema_idx < tables_idx);
        assert!(types_idx < tables_idx);
    }

    #[test]
    fn test_circular_dependency_detection() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        write_file(
            schema_dir,
            "a.sql",
            "-- require: b.sql\nCREATE TABLE a (id INT);",
        );
        write_file(
            schema_dir,
            "b.sql",
            "-- require: a.sql\nCREATE TABLE b (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let result = loader.load_ordered_schema_files();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Circular dependency")
        );
    }

    #[test]
    fn test_missing_dependency() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        write_file(
            schema_dir,
            "tables.sql",
            "-- require: missing.sql\nCREATE TABLE users (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let result = loader.load_ordered_schema_files();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing dependency")
        );
    }

    #[test]
    fn test_circular_dependency_excludes_non_cycle_files() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        // A <-> B form a cycle
        write_file(
            schema_dir,
            "a.sql",
            "-- require: b.sql\nCREATE TABLE a (id INT);",
        );
        write_file(
            schema_dir,
            "b.sql",
            "-- require: a.sql\nCREATE TABLE b (id INT);",
        );

        // C depends on A but is NOT part of the cycle
        write_file(
            schema_dir,
            "c.sql",
            "-- require: a.sql\nCREATE TABLE c (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let result = loader.load_ordered_schema_files();
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("a.sql"));
        assert!(error_msg.contains("b.sql"));
        // c.sql should NOT be mentioned as it's not part of the cycle
        assert!(!error_msg.contains("c.sql"));
    }

    #[test]
    fn test_circular_dependency_shows_cycle_path() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        write_file(
            schema_dir,
            "a.sql",
            "-- require: b.sql\nCREATE TABLE a (id INT);",
        );
        write_file(
            schema_dir,
            "b.sql",
            "-- require: a.sql\nCREATE TABLE b (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let result = loader.load_ordered_schema_files();
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        // Should show the cycle path with arrows
        assert!(error_msg.contains("->"));
    }

    #[test]
    fn test_three_node_circular_dependency() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        // A -> B -> C -> A
        write_file(
            schema_dir,
            "a.sql",
            "-- require: c.sql\nCREATE TABLE a (id INT);",
        );
        write_file(
            schema_dir,
            "b.sql",
            "-- require: a.sql\nCREATE TABLE b (id INT);",
        );
        write_file(
            schema_dir,
            "c.sql",
            "-- require: b.sql\nCREATE TABLE c (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let result = loader.load_ordered_schema_files();
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("a.sql"));
        assert!(error_msg.contains("b.sql"));
        assert!(error_msg.contains("c.sql"));
        assert!(error_msg.contains("->"));
    }

    #[test]
    fn test_multiple_independent_cycles() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        // First cycle: A <-> B
        write_file(
            schema_dir,
            "a.sql",
            "-- require: b.sql\nCREATE TABLE a (id INT);",
        );
        write_file(
            schema_dir,
            "b.sql",
            "-- require: a.sql\nCREATE TABLE b (id INT);",
        );

        // Second cycle: X <-> Y
        write_file(
            schema_dir,
            "x.sql",
            "-- require: y.sql\nCREATE TABLE x (id INT);",
        );
        write_file(
            schema_dir,
            "y.sql",
            "-- require: x.sql\nCREATE TABLE y (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let result = loader.load_ordered_schema_files();
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        // Should mention multiple cycles
        assert!(error_msg.contains("Multiple circular dependencies"));
    }

    #[test]
    fn test_dependent_on_cycle_not_reported() {
        let temp_dir = create_test_schema_dir();
        let schema_dir = temp_dir.path();

        // Base file with no dependencies (will be processed)
        write_file(schema_dir, "base.sql", "CREATE SCHEMA base;");

        // Cycle: A <-> B (A also depends on base)
        write_file(
            schema_dir,
            "a.sql",
            "-- require: base.sql, b.sql\nCREATE TABLE a (id INT);",
        );
        write_file(
            schema_dir,
            "b.sql",
            "-- require: a.sql\nCREATE TABLE b (id INT);",
        );

        // C depends on A (part of cycle) - C is blocked but not in cycle
        write_file(
            schema_dir,
            "c.sql",
            "-- require: a.sql\nCREATE TABLE c (id INT);",
        );

        // D depends on C (transitively depends on cycle) - also blocked
        write_file(
            schema_dir,
            "d.sql",
            "-- require: c.sql\nCREATE TABLE d (id INT);",
        );

        let config = SchemaLoaderConfig::new(schema_dir.to_path_buf());
        let loader = SchemaLoader::new(config);

        let result = loader.load_ordered_schema_files();
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();

        // Only a.sql and b.sql are in the cycle
        assert!(error_msg.contains("a.sql"));
        assert!(error_msg.contains("b.sql"));

        // These should NOT be mentioned
        assert!(!error_msg.contains("base.sql"));
        assert!(!error_msg.contains("c.sql"));
        assert!(!error_msg.contains("d.sql"));
    }
}

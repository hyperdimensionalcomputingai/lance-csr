// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Graph patterns inside SQL, after DuckPGQ's `GRAPH_TABLE` clause.
//!
//! ```sql
//! SELECT name, count(*) FROM GRAPH_TABLE (
//!     MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30
//!     COLUMNS (b.name AS name)
//! ) GROUP BY name
//! ```
//!
//! [`rewrite_graph_table`] turns each clause into a call to the `graph_table` table
//! function with the equivalent Cypher text (`MATCH ... WHERE ... RETURN <columns>`).
//! [`GraphTableFunction`] plans that text with the Cypher planner and returns the plan
//! as a view, which DataFusion inlines into the SQL plan. Hops covered by the CSR are
//! therefore `CsrExpand` nodes in the SQL plan, and the CSR optimizer rules apply to them.
//!
//! Only `GRAPH_TABLE`'s shape comes from SQL/PGQ. The pattern, `WHERE` and `COLUMNS`
//! bodies are Cypher, and there is no `CREATE PROPERTY GRAPH`: the graph is the
//! engine's `GraphConfig`.

use std::sync::Arc;

use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::error::{GraphError, Result};

/// Name the `GRAPH_TABLE` clause is rewritten to; also callable directly with a Cypher string.
pub const GRAPH_TABLE_FUNCTION: &str = "graph_table";

/// Plans one Cypher query into an unoptimized DataFusion plan.
pub type CypherPlanner = dyn Fn(&str) -> Result<LogicalPlan> + Send + Sync;

/// `graph_table('<cypher>')`: the Cypher query's result as a table.
pub struct GraphTableFunction {
    plan: Box<CypherPlanner>,
}

impl GraphTableFunction {
    pub fn new(plan: impl Fn(&str) -> Result<LogicalPlan> + Send + Sync + 'static) -> Self {
        Self {
            plan: Box::new(plan),
        }
    }
}

impl std::fmt::Debug for GraphTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GraphTableFunction")
    }
}

impl TableFunctionImpl for GraphTableFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let text = match args.exprs() {
            [Expr::Literal(value, _)] => value.try_as_str().flatten(),
            _ => None,
        }
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{GRAPH_TABLE_FUNCTION}() takes one string literal: a Cypher query"
            ))
        })?;
        let plan = (self.plan)(text).map_err(|e| DataFusionError::Plan(e.to_string()))?;
        Ok(Arc::new(ViewTable::new(plan, Some(text.to_string()))))
    }
}

/// Replace every `GRAPH_TABLE ( [graph] MATCH ... COLUMNS (...) )` clause in `sql`
/// with `graph_table('MATCH ... RETURN ...')`.
///
/// A direct call, `graph_table('...')`, is left as written. Quoted strings and
/// identifiers are skipped, so `GRAPH_TABLE` inside them is not a clause.
pub fn rewrite_graph_table(sql: &str) -> Result<String> {
    const KEYWORD: &str = "graph_table";
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let (mut copied, mut i) = (0, 0);
    while i < b.len() {
        if is_quote(b[i]) {
            i = skip_quoted(b, i)?;
        } else if is_word(b, i, KEYWORD) {
            let open = skip_whitespace(b, i + KEYWORD.len());
            if b.get(open) != Some(&b'(') {
                i += KEYWORD.len();
                continue;
            }
            let close = matching_paren(b, open)?;
            let body = &sql[open + 1..close];
            if !body.trim_start().starts_with('\'') {
                out.push_str(&sql[copied..i]);
                out.push_str(GRAPH_TABLE_FUNCTION);
                out.push_str("('");
                out.push_str(&to_cypher(body)?.replace('\'', "''"));
                out.push_str("')");
                copied = close + 1;
            }
            i = close + 1;
        } else {
            i += 1;
        }
    }
    out.push_str(&sql[copied..]);
    Ok(out)
}

/// `[graph] MATCH <pattern> [WHERE <cond>] COLUMNS (<exprs>)` -> `MATCH ... RETURN <exprs>`.
fn to_cypher(body: &str) -> Result<String> {
    let b = body.as_bytes();
    let match_at = *top_level_words(b, "match")?
        .first()
        .ok_or_else(|| syntax_error("GRAPH_TABLE needs a MATCH clause"))?;
    // shortcut: a graph name before MATCH is accepted and ignored; an engine holds one graph.
    let graph = body[..match_at].trim();
    if graph.split_whitespace().count() > 1 {
        return Err(syntax_error(format!(
            "unexpected '{graph}' before MATCH in GRAPH_TABLE"
        )));
    }
    let columns_at = top_level_words(b, "columns")?
        .into_iter()
        .rev()
        .find(|&at| at > match_at)
        .ok_or_else(|| syntax_error("GRAPH_TABLE needs a COLUMNS (...) list after MATCH"))?;
    let open = skip_whitespace(b, columns_at + "columns".len());
    if b.get(open) != Some(&b'(') {
        return Err(syntax_error(
            "COLUMNS must be followed by a parenthesized list",
        ));
    }
    let close = matching_paren(b, open)?;
    if !body[close + 1..].trim().is_empty() {
        return Err(syntax_error(format!(
            "unexpected '{}' after COLUMNS (...) in GRAPH_TABLE",
            body[close + 1..].trim()
        )));
    }
    let columns = body[open + 1..close].trim();
    if columns.is_empty() {
        return Err(syntax_error("COLUMNS (...) is empty"));
    }
    sql_identifiers(&format!(
        "{} RETURN {columns}",
        body[match_at..columns_at].trim()
    ))
}

/// SQL/PGQ quoting in Cypher text: each `"name"` (with `""` for `"`) becomes the Cypher
/// identifier `` `name` `` (with ``` `` ``` for `` ` ``). `'...'` strings and `` `...` `` names are kept.
fn sql_identifiers(text: &str) -> Result<String> {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let (mut copied, mut i) = (0, 0);
    while i < b.len() {
        if !is_quote(b[i]) {
            i += 1;
            continue;
        }
        let end = skip_quoted(b, i)?;
        if b[i] == b'"' {
            out.push_str(&text[copied..i]);
            let name = text[i + 1..end - 1].replace("\"\"", "\"");
            out.push('`');
            out.push_str(&name.replace('`', "``"));
            out.push('`');
            copied = end;
        }
        i = end;
    }
    out.push_str(&text[copied..]);
    Ok(out)
}

/// Byte offsets of `word` outside quotes and brackets.
fn top_level_words(b: &[u8], word: &str) -> Result<Vec<usize>> {
    let (mut found, mut depth, mut i) = (Vec::new(), 0usize, 0);
    while i < b.len() {
        match b[i] {
            c if is_quote(c) => {
                i = skip_quoted(b, i)?;
                continue;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            _ if depth == 0 && is_word(b, i, word) => found.push(i),
            _ => {}
        }
        i += 1;
    }
    Ok(found)
}

/// Offset of the `)` closing the `(` at `open`.
fn matching_paren(b: &[u8], open: usize) -> Result<usize> {
    let (mut depth, mut i) = (0usize, open);
    while i < b.len() {
        match b[i] {
            c if is_quote(c) => {
                i = skip_quoted(b, i)?;
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(syntax_error("unbalanced parentheses in GRAPH_TABLE"))
}

fn is_quote(c: u8) -> bool {
    matches!(c, b'\'' | b'"' | b'`')
}

/// Offset just past the quoted string or identifier starting at `i`; a doubled quote is an escape.
// shortcut: Cypher's backslash escapes (`'it\'s'`) aren't recognized; use a doubled quote instead.
fn skip_quoted(b: &[u8], i: usize) -> Result<usize> {
    let quote = b[i];
    let mut j = i + 1;
    while j < b.len() {
        if b[j] == quote {
            if b.get(j + 1) == Some(&quote) {
                j += 2;
                continue;
            }
            return Ok(j + 1);
        }
        j += 1;
    }
    Err(syntax_error("unterminated quote"))
}

fn skip_whitespace(b: &[u8], mut i: usize) -> usize {
    while b.get(i).is_some_and(u8::is_ascii_whitespace) {
        i += 1;
    }
    i
}

/// Whether `word` starts at `i` as a whole identifier, ignoring case.
fn is_word(b: &[u8], i: usize, word: &str) -> bool {
    let ident = |c: &u8| c.is_ascii_alphanumeric() || *c == b'_';
    b.get(i..i + word.len())
        .is_some_and(|s| s.eq_ignore_ascii_case(word.as_bytes()))
        && (i == 0 || !ident(&b[i - 1]))
        && !b.get(i + word.len()).is_some_and(ident)
}

fn syntax_error(message: impl Into<String>) -> GraphError {
    GraphError::PlanError {
        message: message.into(),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_clause_to_cypher_call() {
        let sql = "SELECT * FROM GRAPH_TABLE (g MATCH (a:Person {name: 'Al''s'})-[:KNOWS]->(b) \
                   WHERE a.age > 30 COLUMNS (a.name AS x, count(b) AS n)) t WHERE t.n > 1";
        assert_eq!(
            rewrite_graph_table(sql).unwrap(),
            "SELECT * FROM graph_table('MATCH (a:Person {name: ''Al''''s''})-[:KNOWS]->(b) \
             WHERE a.age > 30 RETURN a.name AS x, count(b) AS n') t WHERE t.n > 1"
        );
    }

    #[test]
    fn leaves_direct_calls_quotes_and_names_alone() {
        for sql in [
            "SELECT * FROM graph_table('MATCH (a) RETURN a.id')",
            "SELECT 'GRAPH_TABLE (x)', \"graph_table\" FROM t",
            "SELECT my_graph_table FROM t",
        ] {
            assert_eq!(rewrite_graph_table(sql).unwrap(), sql);
        }
    }

    #[test]
    fn double_quotes_name_identifiers() {
        let sql = "SELECT * FROM GRAPH_TABLE (MATCH (\"match\":\"Device Type\") \
                   WHERE \"match\".\"asset id\" = 'a\"b' \
                   COLUMNS (\"match\".\"a\"\"b`c\" AS \"select\"))";
        assert_eq!(
            rewrite_graph_table(sql).unwrap(),
            "SELECT * FROM graph_table('MATCH (`match`:`Device Type`) \
             WHERE `match`.`asset id` = ''a\"b'' RETURN `match`.`a\"b``c` AS `select`')"
        );
    }

    #[test]
    fn rewrites_several_clauses() {
        let sql = "SELECT * FROM GRAPH_TABLE(MATCH (a) COLUMNS (a.id)) x \
                   JOIN graph_table (MATCH (b) COLUMNS (b.id)) y ON true";
        assert_eq!(
            rewrite_graph_table(sql).unwrap(),
            "SELECT * FROM graph_table('MATCH (a) RETURN a.id') x \
             JOIN graph_table('MATCH (b) RETURN b.id') y ON true"
        );
    }

    #[test]
    fn rejects_malformed_clauses() {
        for sql in [
            "SELECT * FROM GRAPH_TABLE (COLUMNS (a.id))",
            "SELECT * FROM GRAPH_TABLE (MATCH (a))",
            "SELECT * FROM GRAPH_TABLE (MATCH (a) COLUMNS ())",
            "SELECT * FROM GRAPH_TABLE (MATCH (a) COLUMNS (a.id) LIMIT 1)",
            "SELECT * FROM GRAPH_TABLE (two words MATCH (a) COLUMNS (a.id))",
            "SELECT * FROM GRAPH_TABLE (MATCH (a) COLUMNS (a.id)",
            "SELECT * FROM GRAPH_TABLE (MATCH (a {n: 'x}) COLUMNS (a.id))",
        ] {
            assert!(rewrite_graph_table(sql).is_err(), "{sql}");
        }
    }
}

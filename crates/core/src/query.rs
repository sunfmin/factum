//! A projection's tables, the queries that read them, and how row values
//! compare. Values compare the way SQLite compares them, so every store
//! answers a query alike: null before numbers before text; numbers by value
//! whatever their JSON form; booleans as 0 and 1; objects and arrays as their
//! JSON text.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A table a projection writes: rows are JSON objects under a key; the
/// declared columns are the fields queries filter and order by, and each
/// index makes one combination of them fast.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub indexes: Vec<Vec<String>>,
}

impl Table {
    pub fn new(name: &str, columns: &[&str]) -> Table {
        Table {
            name: name.into(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            indexes: Vec::new(),
        }
    }

    pub fn index(mut self, columns: &[&str]) -> Table {
        self.indexes
            .push(columns.iter().map(|c| c.to_string()).collect());
        self
    }
}

/// Whether a name may be a projection, table or column name: a letter,
/// then letters, digits or underscores.
pub fn is_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Op {
    pub fn sql(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "<>",
            Op::Lt => "<",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::Ge => ">=",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    pub column: String,
    pub op: Op,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub column: String,
    pub descending: bool,
}

/// Rows of one table: those every filter holds for, in the order asked,
/// then by key; at most `limit` of them.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub table: String,
    pub filters: Vec<Filter>,
    pub order: Vec<Order>,
    pub limit: Option<usize>,
}

impl Query {
    pub fn table(table: &str) -> Query {
        Query {
            table: table.into(),
            ..Query::default()
        }
    }
    pub fn filter(mut self, column: &str, op: Op, value: impl Serialize) -> Query {
        let value = serde_json::to_value(value).unwrap_or(Value::Null);
        self.filters.push(Filter {
            column: column.into(),
            op,
            value,
        });
        self
    }
    pub fn order_by(mut self, column: &str, descending: bool) -> Query {
        self.order.push(Order {
            column: column.into(),
            descending,
        });
        self
    }
    pub fn limit(mut self, limit: usize) -> Query {
        self.limit = Some(limit);
        self
    }
    /// Every column it names, filtered or ordered by.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.filters
            .iter()
            .map(|f| f.column.as_str())
            .chain(self.order.iter().map(|o| o.column.as_str()))
    }
}

/// A row value as a store compares it.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Null,
    Number(f64),
    Text(String),
}

impl Scalar {
    pub fn of(v: Option<&Value>) -> Scalar {
        match v {
            None | Some(Value::Null) => Scalar::Null,
            Some(Value::Bool(b)) => Scalar::Number(if *b { 1.0 } else { 0.0 }),
            Some(Value::Number(n)) => Scalar::Number(n.as_f64().unwrap_or(0.0)),
            Some(Value::String(s)) => Scalar::Text(s.clone()),
            Some(other) => Scalar::Text(other.to_string()),
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Scalar::Null => 0,
            Scalar::Number(_) => 1,
            Scalar::Text(_) => 2,
        }
    }
}

/// The total order rows are sorted by.
pub fn compare(a: &Scalar, b: &Scalar) -> Ordering {
    match (a, b) {
        (Scalar::Number(x), Scalar::Number(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Scalar::Text(x), Scalar::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        _ => a.rank().cmp(&b.rank()),
    }
}

/// Whether a filter holds for a row. A comparison with null never holds.
pub fn holds(row: &Value, filter: &Filter) -> bool {
    let (x, y) = (
        Scalar::of(row.get(&filter.column)),
        Scalar::of(Some(&filter.value)),
    );
    if x == Scalar::Null || y == Scalar::Null {
        return false;
    }
    let c = compare(&x, &y);
    match filter.op {
        Op::Eq => c == Ordering::Equal,
        Op::Ne => c != Ordering::Equal,
        Op::Lt => c == Ordering::Less,
        Op::Le => c != Ordering::Greater,
        Op::Gt => c == Ordering::Greater,
        Op::Ge => c != Ordering::Less,
    }
}

/// Answers a query over rows held in memory, as every store must.
pub fn answer(mut rows: Vec<(String, Value)>, query: &Query) -> Vec<(String, Value)> {
    rows.retain(|(_, row)| query.filters.iter().all(|f| holds(row, f)));
    rows.sort_by(|(ka, a), (kb, b)| {
        for o in &query.order {
            let c = compare(&Scalar::of(a.get(&o.column)), &Scalar::of(b.get(&o.column)));
            let c = if o.descending { c.reverse() } else { c };
            if c != Ordering::Equal {
                return c;
            }
        }
        ka.cmp(kb)
    });
    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
    rows
}

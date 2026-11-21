use sea_orm::{ConnectionTrait, Statement};

#[derive(Debug, serde::Deserialize)]
pub struct PlanNode {
    #[serde(rename = "Node Type")]
    pub kind: String,
    #[serde(rename = "Relation Name")]
    pub relation: Option<String>,
    #[serde(rename = "Alias")]
    pub alias: Option<String>,
    #[serde(rename = "Subplan Name")]
    pub subplan: Option<String>,
    #[serde(rename = "Index Name")]
    pub index: Option<String>,
    #[serde(rename = "Index Cond")]
    index_condition: Option<String>,
    #[serde(rename = "Plans", default)]
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn walk(&self) -> Vec<&Self> {
        let mut nodes = vec![self];
        let mut cursor = 0;
        while cursor < nodes.len() {
            nodes.extend(&nodes[cursor].children);
            cursor += 1;
        }
        nodes
    }

    pub fn has_index_constraint(&self, column: &str, operator: &str) -> bool {
        self.index_condition.as_deref().is_some_and(|condition| {
            condition.split('(').any(|expression| {
                let mut tokens = expression.split_whitespace();
                let left = tokens.next().map(|term| term.rsplit('.').next().unwrap());
                left.is_some_and(|term| term.trim_matches('"') == column)
                    && tokens.next() == Some(operator)
            })
        })
    }

    pub fn scoped_index(&self, column: &str) -> bool {
        self.index.is_some() && self.has_index_constraint(column, "=")
    }
}

pub async fn explain<C: ConnectionTrait>(db: &C, mut statement: Statement) -> PlanNode {
    statement
        .sql
        .insert_str(0, "EXPLAIN (FORMAT JSON, COSTS OFF) ");
    let result = db.query_one_raw(statement).await.unwrap().unwrap();
    let document = result
        .try_get::<serde_json::Value>("", "QUERY PLAN")
        .unwrap();
    serde_json::from_value(document[0]["Plan"].clone()).unwrap()
}

pub fn assert_exact_branch_search(branch: &PlanNode, subject: &str, column: &str, by_alias: bool) {
    let nodes = branch.walk();
    let matches = |node: &PlanNode| {
        let label = if by_alias {
            &node.alias
        } else {
            &node.relation
        };
        label.as_deref() == Some(subject)
    };
    assert!(
        nodes
            .iter()
            .any(|node| matches(node) && node.scoped_index(column)),
        "missing exact indexed {column} lookup for {subject}: {branch:#?}"
    );
    assert!(
        !nodes
            .iter()
            .any(|node| matches(node) && node.kind == "Seq Scan"),
        "unexpected sequential scan for {subject}: {branch:#?}"
    );
}

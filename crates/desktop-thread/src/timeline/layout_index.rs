//! Retained order-statistic AVL tree. Coordinates never key item ownership.
use gpui_kit::{Pixels, px};
use pioneer_client::timeline::presentation::RowId;
use std::collections::HashMap;

type Link = Option<usize>;
struct Node {
    id: RowId,
    size: Pixels,
    sum: Pixels,
    count: usize,
    height: i32,
    parent: Link,
    left: Link,
    right: Link,
}
#[derive(Default)]
pub(crate) struct RowLayoutIndex {
    root: Link,
    nodes: Vec<Option<Node>>,
    free: Vec<usize>,
    by_id: HashMap<RowId, usize>,
}
impl RowLayoutIndex {
    fn node(&self, ix: usize) -> &Node {
        self.nodes[ix].as_ref().unwrap()
    }
    fn node_mut(&mut self, ix: usize) -> &mut Node {
        self.nodes[ix].as_mut().unwrap()
    }
    fn count(&self, link: Link) -> usize {
        link.map_or(0, |ix| self.node(ix).count)
    }
    fn height(&self, link: Link) -> i32 {
        link.map_or(0, |ix| self.node(ix).height)
    }
    fn sum(&self, link: Link) -> Pixels {
        link.map_or(px(0.), |ix| self.node(ix).sum)
    }
    pub(crate) fn len(&self) -> usize {
        self.count(self.root)
    }
    fn left(&mut self, ix: usize, child: Link) {
        self.node_mut(ix).left = child;
        if let Some(child) = child {
            self.node_mut(child).parent = Some(ix);
        }
    }
    fn right(&mut self, ix: usize, child: Link) {
        self.node_mut(ix).right = child;
        if let Some(child) = child {
            self.node_mut(child).parent = Some(ix);
        }
    }
    fn refresh(&mut self, ix: usize) {
        let n = self.node(ix);
        let (count, height, sum) = (
            1 + self.count(n.left) + self.count(n.right),
            1 + self.height(n.left).max(self.height(n.right)),
            self.sum(n.left) + n.size + self.sum(n.right),
        );
        let n = self.node_mut(ix);
        n.count = count;
        n.height = height;
        n.sum = sum;
    }
    fn rotate_left(&mut self, ix: usize) -> usize {
        let parent = self.node(ix).parent;
        let next = self.node(ix).right.unwrap();
        self.right(ix, self.node(next).left);
        self.left(next, Some(ix));
        self.node_mut(next).parent = parent;
        self.refresh(ix);
        self.refresh(next);
        next
    }
    fn rotate_right(&mut self, ix: usize) -> usize {
        let parent = self.node(ix).parent;
        let next = self.node(ix).left.unwrap();
        self.left(ix, self.node(next).right);
        self.right(next, Some(ix));
        self.node_mut(next).parent = parent;
        self.refresh(ix);
        self.refresh(next);
        next
    }
    fn balance(&mut self, ix: usize) -> usize {
        self.refresh(ix);
        let n = self.node(ix);
        let balance = self.height(n.left) - self.height(n.right);
        if balance > 1 {
            let left = self.node(ix).left.unwrap();
            if self.height(self.node(left).left) < self.height(self.node(left).right) {
                let next = self.rotate_left(left);
                self.left(ix, Some(next));
            }
            self.rotate_right(ix)
        } else if balance < -1 {
            let right = self.node(ix).right.unwrap();
            if self.height(self.node(right).right) < self.height(self.node(right).left) {
                let next = self.rotate_right(right);
                self.right(ix, Some(next));
            }
            self.rotate_left(ix)
        } else {
            ix
        }
    }
    fn insert_node(&mut self, root: Link, at: usize, node: usize) -> usize {
        let Some(ix) = root else {
            return node;
        };
        let left_count = self.count(self.node(ix).left);
        if at <= left_count {
            let next = self.insert_node(self.node(ix).left, at, node);
            self.left(ix, Some(next));
        } else {
            let next = self.insert_node(self.node(ix).right, at - left_count - 1, node);
            self.right(ix, Some(next));
        }
        self.balance(ix)
    }
    pub(crate) fn insert(&mut self, at: usize, id: RowId, size: Pixels) {
        assert!(at <= self.len());
        assert!(!self.by_id.contains_key(&id));
        let node = Node {
            id: id.clone(),
            size,
            sum: size,
            count: 1,
            height: 1,
            parent: None,
            left: None,
            right: None,
        };
        let ix = if let Some(ix) = self.free.pop() {
            self.nodes[ix] = Some(node);
            ix
        } else {
            self.nodes.push(Some(node));
            self.nodes.len() - 1
        };
        self.by_id.insert(id, ix);
        self.root = Some(self.insert_node(self.root, at, ix));
        self.node_mut(self.root.unwrap()).parent = None;
    }
    pub(crate) fn rank(&self, id: &RowId) -> Option<usize> {
        let mut ix = *self.by_id.get(id)?;
        let mut rank = self.count(self.node(ix).left);
        while let Some(parent) = self.node(ix).parent {
            if self.node(parent).right == Some(ix) {
                rank += 1 + self.count(self.node(parent).left);
            }
            ix = parent;
        }
        Some(rank)
    }
    fn remove_node(&mut self, ix: usize, at: usize) -> Link {
        let left_count = self.count(self.node(ix).left);
        if at < left_count {
            let next = self.remove_node(self.node(ix).left.unwrap(), at);
            self.left(ix, next);
        } else if at > left_count {
            let next = self.remove_node(self.node(ix).right.unwrap(), at - left_count - 1);
            self.right(ix, next);
        } else {
            let (left, right, parent) = (
                self.node(ix).left,
                self.node(ix).right,
                self.node(ix).parent,
            );
            if left.is_none() || right.is_none() {
                let child = left.or(right);
                if let Some(child) = child {
                    self.node_mut(child).parent = parent;
                }
                let node = self.nodes[ix].take().unwrap();
                self.by_id.remove(&node.id);
                self.free.push(ix);
                return child;
            }
            let mut successor = right.unwrap();
            while let Some(left) = self.node(successor).left {
                successor = left;
            }
            let id = self.node(successor).id.clone();
            let size = self.node(successor).size;
            let old_id = self.node(ix).id.clone();
            let next = self.remove_node(right.unwrap(), 0);
            self.right(ix, next);
            self.by_id.remove(&old_id);
            self.by_id.insert(id.clone(), ix);
            self.node_mut(ix).id = id;
            self.node_mut(ix).size = size;
        }
        Some(self.balance(ix))
    }
    pub(crate) fn remove(&mut self, id: &RowId) -> bool {
        let Some(at) = self.rank(id) else {
            return false;
        };
        self.root = self.remove_node(self.root.unwrap(), at);
        if let Some(root) = self.root {
            self.node_mut(root).parent = None;
        }
        true
    }
    pub(crate) fn update(&mut self, id: &RowId, size: Pixels) -> bool {
        let Some(&ix) = self.by_id.get(id) else {
            return false;
        };
        if self.node(ix).size == size {
            return false;
        }
        self.node_mut(ix).size = size;
        let mut next = Some(ix);
        while let Some(ix) = next {
            self.refresh(ix);
            next = self.node(ix).parent;
        }
        true
    }
    pub(crate) fn origin(&self, at: usize) -> Option<Pixels> {
        if at > self.len() {
            return None;
        }
        let (mut link, mut rank, mut sum) = (self.root, at, px(0.));
        while let Some(ix) = link {
            let n = self.node(ix);
            let count = self.count(n.left);
            if rank <= count {
                link = n.left;
            } else {
                rank -= count + 1;
                sum += self.sum(n.left) + n.size;
                link = n.right;
            }
        }
        Some(sum)
    }
    pub(crate) fn end(&self, at: usize) -> Option<Pixels> {
        if at >= self.len() {
            None
        } else {
            self.origin(at + 1)
        }
    }
    /// First item whose lower edge is strictly beyond the content offset.
    pub(crate) fn lower_bound(&self, offset: Pixels) -> usize {
        let (mut link, mut rank, mut sum) = (self.root, 0, px(0.));
        while let Some(ix) = link {
            let n = self.node(ix);
            let left = self.sum(n.left);
            if offset < sum + left {
                link = n.left;
            } else if offset < sum + left + n.size {
                return rank + self.count(n.left);
            } else {
                rank += self.count(n.left) + 1;
                sum += left + n.size;
                link = n.right;
            }
        }
        rank
    }
}

pub(crate) fn visible_range(
    index: &RowLayoutIndex,
    offset: Pixels,
    height: Pixels,
) -> std::ops::Range<usize> {
    let start = index.lower_bound(offset.max(px(0.)));
    let end = (index.lower_bound(offset.max(px(0.)) + height.max(px(0.))) + 2).min(index.len());
    start.min(end)..end
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(ix: usize) -> RowId {
        serde_json::from_value(serde_json::json!(format!("item:{ix}"))).unwrap()
    }
    #[test]
    fn mutations_preserve_semantic_positions_and_prefixes() {
        let mut index = RowLayoutIndex::default();
        let mut expected = Vec::new();
        for ix in 0..2000 {
            let at = (ix * 997) % (ix + 1);
            index.insert(at, id(ix), px((ix % 17 + 1) as f32));
            expected.insert(at, (ix, (ix % 17 + 1) as f32));
        }
        for ix in (0..2000).step_by(3) {
            assert!(index.remove(&id(ix)));
            expected.retain(|(key, _)| *key != ix);
        }
        for ix in (1..2000).step_by(7) {
            index.update(&id(ix), px(50.));
            if let Some(row) = expected.iter_mut().find(|(key, _)| *key == ix) {
                row.1 = 50.;
            }
        }
        let mut total = 0.;
        for (at, (key, height)) in expected.iter().enumerate() {
            assert_eq!(index.rank(&id(*key)), Some(at));
            assert_eq!(index.origin(at), Some(px(total)));
            assert_eq!(index.lower_bound(px(total)), at);
            total += height;
            assert_eq!(index.end(at), Some(px(total)));
        }
        assert_eq!(index.origin(index.len()), Some(px(total)));
        assert!(index.height(index.root) < 20);
        assert_eq!(
            index.nodes.iter().filter(|n| n.is_some()).count(),
            expected.len()
        );
    }
    #[test]
    fn range_keeps_stock_trailing_overscan_and_is_pure() {
        let mut index = RowLayoutIndex::default();
        for ix in 0..5 {
            index.insert(ix, id(ix), px(10.));
        }
        assert_eq!(visible_range(&index, px(10.), px(10.)), 1..4);
        assert_eq!(visible_range(&index, px(40.), px(100.)), 4..5);
        assert_eq!(index.origin(5), Some(px(50.)));
    }
}

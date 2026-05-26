use std::collections::HashMap;
use crate::error::FerroError;
pub struct Node {
    pub key: u32,
    pub next: Option<usize>,
    pub prev: Option<usize>,
}
pub struct LinkedHashSet {
    pub map: HashMap<u32, usize>, // key -> index in nodes
    pub nodes: Vec<Node>,
    pub tail: Option<usize>, // back (LRU)
    pub head: Option<usize>, // front (MRU)
    pub free_slots: Vec<usize> // reusable slots from unused nodes
}

impl LinkedHashSet {

    pub fn new(map: HashMap<u32, usize>, nodes: Vec<Node>, tail: Option<usize>, head: Option<usize>, free_slots: Vec<usize>) -> Self {
        LinkedHashSet { map, nodes, tail, head, free_slots }
    }

    // inserts to front
    pub fn insert(&mut self, key: u32) -> Result<(), FerroError>{
        if self.map.contains_key(&key) {
            self.move_to_front(key)?;
            return Ok(());
        }
        let i;

        match self.free_slots.pop() {
            Some(slot) => {
                self.nodes[slot] = Node::new(key, self.head, None);
                i = slot;
            }
            None => {
                i = self.nodes.len();
                self.nodes.push(Node::new(key, self.head, None));
            }
        };

        match self.head {
            Some(old_head) => {
                self.nodes[old_head].prev = Some(i);
            }
            None => self.tail = Some(i)
        };

        self.head = Some(i);
        self.map.insert(key, i);
        Ok(())
    }

    pub fn contains(&self, key: u32) -> bool{
        self.map.contains_key(&key)
    }

    // make prev point to next, and vice versa 
    pub fn remove(&mut self, key: u32) -> Result<(), FerroError>{
        let i = match self.map.remove(&key) {
            Some(ind) => ind,
            None => return Err(FerroError::KeyNotFound)
        };

        match self.nodes[i].prev {
            Some(p) => self.nodes[p].next = self.nodes[i].next,
            None => self.head = self.nodes[i].next
        }

        match self.nodes[i].next {
            Some(n) => self.nodes[n].prev = self.nodes[i].prev,
            None => self.tail = self.nodes[i].prev
        }

        self.free_slots.push(i);
        Ok(())
    }

    pub fn pop_back(&mut self) -> Result<u32, FerroError> {
        let i = match self.tail {
            Some(tail) => tail,
            None => return Err(FerroError::EmptyList)
        };

        match self.nodes[i].prev {
            Some(p) => self.nodes[p].next = None,
            None => self.head = None
        }
        self.tail = self.nodes[i].prev;
        self.map.remove(&self.nodes[i].key);
        self.free_slots.push(i);
        Ok(self.nodes[i].key)
    }

    pub fn move_to_front(&mut self, key: u32) -> Result<(), FerroError>{
        let i = match self.map.get(&key) {
            Some(&i) => i,
            None => return Err(FerroError::KeyNotFound),
        };

        let prev = self.nodes[i].prev;
        let next = self.nodes[i].next;

        match prev {
            Some(p) => self.nodes[p].next = next,
            None => self.head = next
        }

        match next  {
            Some(n) => self.nodes[n].prev = prev,
            None => self.tail = prev
        }

        self.nodes[i].prev = None;
        self.nodes[i].next = self.head;

        match self.head {
            Some(old_head) => self.nodes[old_head].prev = Some(i),
            None => self.tail = Some(i)
        }

        self.head = Some(i);
        Ok(())
    }
}

impl Node {
    pub fn new(key: u32, next: Option<usize>, prev: Option<usize>) -> Self{
        Node { key, next, prev }
    }
}
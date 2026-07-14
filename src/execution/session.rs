pub struct Session {
    pub current: Option<u64>,
}

impl Session {
    pub fn new() -> Self {
        Self { current: None }
    }
}
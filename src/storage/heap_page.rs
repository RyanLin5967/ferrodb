pub struct SlotEntry {
    offset: u16,
    length: u16
}

pub struct Page {
    page_id: u32,
    num_slots: u16,
    slot_arr: Vec<SlotEntry>,
    tuples: Vec<u8>
}


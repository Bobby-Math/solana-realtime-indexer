use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct SubscriptionManager {
    pub account_subs: HashMap<String, usize>,
    pub program_subs: HashMap<String, usize>,
    pub slot_subs: usize,
}

extern crate multimap;
use multimap::MultiMap;
use utils::app_config::AppConfig;

use crate::structs::{GDPName, GDPNameRecord, GDPStatus, GDPNameRecordType::*};

// an interface to rib
// will support queries
// a CRDT based RIB
pub struct RoutingInformationBase {
    pub routing_table: MultiMap<GDPName, GDPNameRecord>,
}

impl RoutingInformationBase {
    pub fn new() -> RoutingInformationBase {
        // TODO: config can populate the RIB somehow

        RoutingInformationBase {
            routing_table: MultiMap::new()
        }
    }

    pub fn put(&mut self, key: GDPName, value: GDPNameRecord) -> Option<()> {
        self.routing_table.insert(key, value);
        Some(())
    }

    pub fn get(&self, key: GDPName) -> Option<&Vec<GDPNameRecord>> {
        self.routing_table.get_vec(&key)
    }
}

//
pub async fn local_rib_handler(
    mut rib_query_rx: tokio::sync::mpsc::UnboundedReceiver<GDPNameRecord>,
    rib_response_tx: tokio::sync::mpsc::UnboundedSender<GDPNameRecord>,
    stat_tx: tokio::sync::mpsc::UnboundedSender<GDPStatus>,   
) {
    // TODO: currently, we only take one rx due to select! limitation
    // will use FutureUnordered Instead
    let _receive_handle = tokio::spawn(async move {
        let mut rib_store = RoutingInformationBase::new();

        // loop polling from
        loop {
            tokio::select! {
                // GDP Name Record received
                Some(query) = rib_query_rx.recv() => {
                    match query.record_type {
                        EMPTY => {
                            warn!("received empty RIB query")
                        },
                        QUERY => {
                            match rib_store.get(query.gdpname) {
                                Some(records) => {
                                    for record in records {
                                        rib_response_tx.send(record.clone()).expect(
                                            "failed to send RIB query response"
                                        );
                                    }
                                },
                                None => {
                                    warn!("received RIB query for non-existing name");
                                    rib_response_tx.send(
                                        GDPNameRecord{
                                            record_type: EMPTY,
                                            gdpname: query.gdpname, 
                                            webrtc_offer: None, 
                                            ip_address: None, 
                                            indirect: None, 
                                        }
                                    ).expect(
                                        "failed to send RIB query response"
                                    );
                                }
                            }
                        },
                        MERGE => {},
                        UPDATE => {},
                        DELETE => {},
                    }
                }
            }
        }
    });
}

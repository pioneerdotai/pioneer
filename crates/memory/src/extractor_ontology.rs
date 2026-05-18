use crate::quality::MemoryOntologyClassification;
use pioneer_protocol::{
    MemoryEvidenceClass, MemoryFactClass, MemoryLifetimeClass, MemoryOwnershipClass,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const EXTRACTOR_ONTOLOGY_PROPOSAL_METADATA_KEY: &str = "extractor_ontology_proposal";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemoryExtractorOntologyProposal {
    pub fact_class: MemoryFactClass,
    pub lifetime_class: MemoryLifetimeClass,
    pub evidence_class: MemoryEvidenceClass,
    pub proposed_ownership_class: MemoryOwnershipClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct MemoryExtractorOntologyProposalComparison {
    pub fact_class_matches: bool,
    pub lifetime_class_matches: bool,
    pub evidence_class_matches: bool,
    pub ownership_class_matches: bool,
    pub all_match: bool,
}

impl MemoryExtractorOntologyProposal {
    pub(crate) fn compare_to_service_classification(
        self,
        ontology: &MemoryOntologyClassification,
        evidence_class: MemoryEvidenceClass,
    ) -> MemoryExtractorOntologyProposalComparison {
        let comparison = MemoryExtractorOntologyProposalComparison {
            fact_class_matches: self.fact_class == ontology.fact_class,
            lifetime_class_matches: self.lifetime_class == ontology.lifetime_class,
            evidence_class_matches: self.evidence_class == evidence_class,
            ownership_class_matches: self.proposed_ownership_class
                == ontology.proposed_ownership_class,
            all_match: false,
        };
        MemoryExtractorOntologyProposalComparison {
            all_match: comparison.fact_class_matches
                && comparison.lifetime_class_matches
                && comparison.evidence_class_matches
                && comparison.ownership_class_matches,
            ..comparison
        }
    }
}

pub(crate) fn insert_extractor_ontology_proposal_metadata(
    metadata: &mut BTreeMap<String, serde_json::Value>,
    proposal: MemoryExtractorOntologyProposal,
) {
    if let Ok(value) = serde_json::to_value(proposal) {
        metadata.insert(EXTRACTOR_ONTOLOGY_PROPOSAL_METADATA_KEY.to_owned(), value);
    }
}

pub(crate) fn extractor_ontology_proposal_from_metadata(
    metadata: &BTreeMap<String, serde_json::Value>,
) -> Option<MemoryExtractorOntologyProposal> {
    metadata
        .get(EXTRACTOR_ONTOLOGY_PROPOSAL_METADATA_KEY)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn proposal_has_unknown_class(proposal: &MemoryExtractorOntologyProposal) -> bool {
    proposal.fact_class == MemoryFactClass::Unknown
        || proposal.lifetime_class == MemoryLifetimeClass::Unknown
}

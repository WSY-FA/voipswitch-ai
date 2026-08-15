use crate::{AsrProvider, LlmProvider, TtsProvider};
use ai_protocol::id::ProviderId;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Default)]
pub struct ProviderRegistry {
    asr: BTreeMap<ProviderId, Arc<dyn AsrProvider>>,
    llm: BTreeMap<ProviderId, Arc<dyn LlmProvider>>,
    tts: BTreeMap<ProviderId, Arc<dyn TtsProvider>>,
}

impl ProviderRegistry {
    pub fn register_asr(&mut self, provider: Arc<dyn AsrProvider>) -> Result<(), String> {
        let provider_id = provider.provider_id().clone();
        if self.asr.insert(provider_id.clone(), provider).is_some() {
            return Err(format!("duplicate ASR provider {provider_id}"));
        }
        Ok(())
    }

    pub fn register_llm(&mut self, provider: Arc<dyn LlmProvider>) -> Result<(), String> {
        let provider_id = provider.provider_id().clone();
        if self.llm.insert(provider_id.clone(), provider).is_some() {
            return Err(format!("duplicate LLM provider {provider_id}"));
        }
        Ok(())
    }

    pub fn register_tts(&mut self, provider: Arc<dyn TtsProvider>) -> Result<(), String> {
        let provider_id = provider.provider_id().clone();
        if self.tts.insert(provider_id.clone(), provider).is_some() {
            return Err(format!("duplicate TTS provider {provider_id}"));
        }
        Ok(())
    }

    pub fn asr(&self, provider_id: &str) -> Option<Arc<dyn AsrProvider>> {
        self.asr
            .iter()
            .find_map(|(id, provider)| (id.as_str() == provider_id).then(|| provider.clone()))
    }

    pub fn llm(&self, provider_id: &str) -> Option<Arc<dyn LlmProvider>> {
        self.llm
            .iter()
            .find_map(|(id, provider)| (id.as_str() == provider_id).then(|| provider.clone()))
    }

    pub fn tts(&self, provider_id: &str) -> Option<Arc<dyn TtsProvider>> {
        self.tts
            .iter()
            .find_map(|(id, provider)| (id.as_str() == provider_id).then(|| provider.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockAsrProvider;

    #[test]
    fn rejects_duplicate_provider_ids_per_capability() {
        let id = ProviderId::new("mock-asr").unwrap();
        let mut registry = ProviderRegistry::default();
        registry
            .register_asr(Arc::new(MockAsrProvider::new(id.clone())))
            .unwrap();
        assert!(
            registry
                .register_asr(Arc::new(MockAsrProvider::new(id)))
                .is_err()
        );
    }
}

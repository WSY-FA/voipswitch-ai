use ai_protocol::control::{ConversationState, JobRef};
use anyhow::{Result, bail};

#[derive(Debug, Clone)]
pub struct VoiceAgentSession {
    pub conversation: JobRef,
    state: ConversationState,
    pub playback_generation: u64,
}

impl VoiceAgentSession {
    pub fn start(conversation: JobRef) -> Result<Self> {
        if conversation.generation == 0 {
            bail!("conversation generation must be greater than zero");
        }
        Ok(Self {
            conversation,
            state: ConversationState::Starting,
            playback_generation: 1,
        })
    }
    pub fn state(&self) -> ConversationState {
        self.state
    }
    pub fn ready(&mut self) -> Result<()> {
        self.transition(ConversationState::Listening)
    }
    pub fn begin_thinking(&mut self) -> Result<()> {
        self.transition(ConversationState::Thinking)
    }
    pub fn begin_speaking(&mut self) -> Result<u64> {
        self.transition(ConversationState::Speaking)?;
        Ok(self.playback_generation)
    }
    pub fn barge_in(&mut self) -> Result<u64> {
        if self.state != ConversationState::Speaking {
            bail!("barge-in is only valid while speaking");
        }
        self.playback_generation = self.playback_generation.saturating_add(1);
        self.transition(ConversationState::Listening)?;
        Ok(self.playback_generation)
    }
    pub fn stop(&mut self) -> Result<()> {
        if matches!(
            self.state,
            ConversationState::Stopped | ConversationState::Stopping
        ) {
            return Ok(());
        }
        self.transition(ConversationState::Stopping)?;
        self.transition(ConversationState::Stopped)
    }
    fn transition(&mut self, next: ConversationState) -> Result<()> {
        let valid = matches!(
            (self.state, next),
            (ConversationState::Starting, ConversationState::Listening)
                | (ConversationState::Listening, ConversationState::Thinking)
                | (ConversationState::Thinking, ConversationState::Speaking)
                | (ConversationState::Speaking, ConversationState::Listening)
                | (ConversationState::Listening, ConversationState::Stopping)
                | (ConversationState::Thinking, ConversationState::Stopping)
                | (ConversationState::Speaking, ConversationState::Stopping)
                | (ConversationState::Stopping, ConversationState::Stopped)
        );
        if !valid {
            bail!(
                "invalid voice-agent transition {:?} -> {:?}",
                self.state,
                next
            );
        }
        self.state = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_protocol::id::{ConversationId, JobId, OperationId, TenantId};
    fn job() -> JobRef {
        JobRef {
            job_id: JobId::new("job-1").unwrap(),
            tenant_id: TenantId::new("tenant-1").unwrap(),
            conversation_id: ConversationId::new("conversation-1").unwrap(),
            operation_id: OperationId::new("operation-1").unwrap(),
            generation: 1,
        }
    }
    #[test]
    fn barge_in_advances_playback_generation() {
        let mut session = VoiceAgentSession::start(job()).unwrap();
        session.ready().unwrap();
        session.begin_thinking().unwrap();
        let first = session.begin_speaking().unwrap();
        let second = session.barge_in().unwrap();
        assert_eq!(second, first + 1);
        assert_eq!(session.state(), ConversationState::Listening);
    }
}

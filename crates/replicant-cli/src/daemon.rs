use std::{env, io};

use replicant_protocol::{
    ApproveRefreshRequest, AutomationResetRequest, AutomationResetResponse, DaemonHealth,
    DescriptorCatalog, ErrorResponse, RefreshRunDetail, RefreshRunSummary, RunOperationRequest,
    RunOperationResponse, StartRefreshRequest, StartWorkflowRequest, StartWorkflowResponse,
    Versioned, WorkflowControlResponse, WorkflowDetail, WorkflowListResponse,
};
use serde::{Serialize, de::DeserializeOwned};

const DEFAULT_URL: &str = "http://127.0.0.1:8080";

pub(crate) struct DaemonClient {
    base_url: String,
    http: reqwest::Client,
}

impl DaemonClient {
    pub(crate) fn from_env() -> Self {
        Self::new(env::var("REPLICANTD_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned()))
    }

    fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
        }
    }

    pub(crate) async fn health(&self) -> crate::AnyResult<DaemonHealth> {
        self.get("/api/health").await
    }

    pub(crate) async fn workflows(&self) -> crate::AnyResult<WorkflowListResponse> {
        self.get("/api/workflows").await
    }

    pub(crate) async fn descriptors(&self) -> crate::AnyResult<DescriptorCatalog> {
        self.get("/api/descriptors").await
    }

    pub(crate) async fn workflow(&self, id: &str) -> crate::AnyResult<WorkflowDetail> {
        self.get(&format!("/api/workflows/{id}")).await
    }

    pub(crate) async fn start(
        &self,
        request: &StartWorkflowRequest,
    ) -> crate::AnyResult<StartWorkflowResponse> {
        self.post("/api/workflows", Some(request)).await
    }

    pub(crate) async fn run_operation(
        &self,
        class: &str,
        kind: &str,
        request: &RunOperationRequest,
    ) -> crate::AnyResult<RunOperationResponse> {
        self.post(&format!("/api/{class}s/{kind}"), Some(request))
            .await
    }

    pub(crate) async fn control(
        &self,
        id: &str,
        command: &str,
    ) -> crate::AnyResult<WorkflowControlResponse> {
        self.post::<(), _>(&format!("/api/workflows/{id}/{command}"), None)
            .await
    }

    pub(crate) async fn automation_reset(
        &self,
        request: &AutomationResetRequest,
    ) -> crate::AnyResult<AutomationResetResponse> {
        self.post("/api/automation/reset", Some(request)).await
    }

    pub(crate) async fn start_refresh(
        &self,
        request: &StartRefreshRequest,
    ) -> crate::AnyResult<RefreshRunSummary> {
        self.post("/api/refreshes", Some(request)).await
    }

    pub(crate) async fn refreshes(&self) -> crate::AnyResult<Vec<RefreshRunSummary>> {
        self.get("/api/refreshes").await
    }

    pub(crate) async fn refresh(&self, id: &str) -> crate::AnyResult<RefreshRunDetail> {
        self.get(&format!("/api/refreshes/{id}")).await
    }

    pub(crate) async fn approve_refresh(
        &self,
        id: &str,
        request: &ApproveRefreshRequest,
    ) -> crate::AnyResult<RefreshRunSummary> {
        self.post(&format!("/api/refreshes/{id}/approve"), Some(request))
            .await
    }

    pub(crate) async fn cancel_refresh(&self, id: &str) -> crate::AnyResult<RefreshRunSummary> {
        self.post::<(), _>(&format!("/api/refreshes/{id}/cancel"), None)
            .await
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> crate::AnyResult<T> {
        self.decode(self.http.get(self.endpoint(path)).send().await?)
            .await
    }

    async fn post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<&B>,
    ) -> crate::AnyResult<T> {
        let request = self.http.post(self.endpoint(path));
        let response = match body {
            Some(body) => request.json(body),
            None => request,
        }
        .send()
        .await?;
        self.decode(response).await
    }

    async fn decode<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> crate::AnyResult<T> {
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            let message = serde_json::from_slice::<Versioned<ErrorResponse>>(&bytes)
                .map(|error| error.payload.message)
                .unwrap_or_else(|_| format!("replicantd returned HTTP {status}"));
            return Err(io::Error::other(message).into());
        }
        let response = serde_json::from_slice::<Versioned<T>>(&bytes)?;
        if response.protocol_version != replicant_protocol::PROTOCOL_VERSION {
            return Err(io::Error::other(format!(
                "unsupported replicantd protocol version {}",
                response.protocol_version
            ))
            .into());
        }
        Ok(response.payload)
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructs_local_endpoints_without_duplicate_slashes() {
        let client = DaemonClient::new("http://127.0.0.1:9000/".to_owned());
        assert_eq!(
            client.endpoint("/api/workflows/abc/pause"),
            "http://127.0.0.1:9000/api/workflows/abc/pause"
        );
    }
    #[tokio::test]
    async fn refresh_commands_map_to_daemon_routes() {
        use replicant_protocol::{RefreshDelta, RefreshPhase, RefreshRunDetail, RefreshRunSummary};
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{body_json, method, path},
        };

        let server = MockServer::start().await;
        let summary = RefreshRunSummary {
            run_id: "run-1".to_owned(),
            mode: "dry_run".to_owned(),
            status: "queued".to_owned(),
            readiness: "unavailable".to_owned(),
            current_phase: None,
            read_requests_per_minute: 30,
            request_attempts: 0,
            delta: RefreshDelta::default(),
            updated_at: 1,
        };
        let start_request = StartRefreshRequest {
            phases: vec![RefreshPhase::Account],
            dry_run: true,
            read_requests_per_minute: Some(30),
        };
        Mock::given(method("POST"))
            .and(path("/api/refreshes"))
            .and(body_json(&start_request))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(Versioned::current(summary.clone())),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/refreshes"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(Versioned::current(vec![summary.clone()])),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/refreshes/run-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(Versioned::current(
                RefreshRunDetail {
                    summary: summary.clone(),
                    requested_phases: vec![RefreshPhase::Account],
                    phases: Vec::new(),
                },
            )))
            .mount(&server)
            .await;
        let approval = ApproveRefreshRequest {
            phase: RefreshPhase::Account,
            digest: "deadbeef".to_owned(),
        };
        Mock::given(method("POST"))
            .and(path("/api/refreshes/run-1/approve"))
            .and(body_json(&approval))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(Versioned::current(summary.clone())),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/refreshes/run-1/cancel"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(Versioned::current(summary.clone())),
            )
            .mount(&server)
            .await;

        let client = DaemonClient::new(server.uri());
        assert_eq!(client.start_refresh(&start_request).await.unwrap(), summary);
        assert_eq!(client.refreshes().await.unwrap(), vec![summary.clone()]);
        assert_eq!(client.refresh("run-1").await.unwrap().summary, summary);
        assert_eq!(
            client.approve_refresh("run-1", &approval).await.unwrap(),
            summary
        );
        assert_eq!(client.cancel_refresh("run-1").await.unwrap(), summary);
    }
}

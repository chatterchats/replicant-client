use std::{env, io};

use replicant_protocol::{
    DaemonHealth, ErrorResponse, StartWorkflowRequest, StartWorkflowResponse, Versioned,
    WorkflowControlResponse, WorkflowDetail, WorkflowListResponse,
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

    pub(crate) async fn workflow(&self, id: &str) -> crate::AnyResult<WorkflowDetail> {
        self.get(&format!("/api/workflows/{id}")).await
    }

    pub(crate) async fn start(
        &self,
        request: &StartWorkflowRequest,
    ) -> crate::AnyResult<StartWorkflowResponse> {
        self.post("/api/workflows", Some(request)).await
    }

    pub(crate) async fn control(
        &self,
        id: &str,
        command: &str,
    ) -> crate::AnyResult<WorkflowControlResponse> {
        self.post::<(), _>(&format!("/api/workflows/{id}/{command}"), None)
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
}

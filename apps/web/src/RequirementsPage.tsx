import type { RequirementSummary } from "./protocol";

export function RequirementsPage({
  requirements,
  onSelectWorkflow,
}: {
  requirements: RequirementSummary[];
  onSelectWorkflow: (id: string) => void;
}) {
  return (
    <article className="page requirements-page">
      <p className="eyebrow">Automation</p>
      <h1>Requirements</h1>
      <p className="lede">
        Desired state stays visible alongside managed state and its child work.
      </p>
      {requirements.length === 0 ? (
        <section className="connection-card">
          <p>No desired-state requirements have been started.</p>
        </section>
      ) : (
        <div className="requirements-grid">
          {requirements.map((requirement) => (
            <button
              className="requirement-card"
              key={requirement.id}
              onClick={() => {
                onSelectWorkflow(requirement.workflow_id);
              }}
            >
              <span>
                <small>{requirement.scope}</small>
                <strong>{requirement.name}</strong>
                <small>{requirement.target}</small>
              </span>
              <dl>
                <div>
                  <dt>Desired</dt>
                  <dd>{requirement.desired}</dd>
                </div>
                <div>
                  <dt>Actual</dt>
                  <dd>{requirement.actual}</dd>
                </div>
                <div>
                  <dt>In progress</dt>
                  <dd>{requirement.in_progress}</dd>
                </div>
                <div>
                  <dt>Missing</dt>
                  <dd>{requirement.missing}</dd>
                </div>
              </dl>
            </button>
          ))}
        </div>
      )}
    </article>
  );
}

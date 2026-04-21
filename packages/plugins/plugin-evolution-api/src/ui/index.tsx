import React from "react";

export function EvolutionSettingsPage() {
  return (
    <div style={{ padding: "1rem" }}>
      <h2>Evolution API Plugin</h2>
      <p>
        Configure the plugin using the settings form above. All fields marked as
        &quot;secret-ref&quot; will be stored securely.
      </p>
      <h3>Setup Guide</h3>
      <ol>
        <li>Enter your Evolution API URL and API key</li>
        <li>Configure the Evolution instance name</li>
        <li>Set up AWS SQS credentials and queue URL for inbound messages</li>
        <li>Enter the PostgreSQL connection URL (same database as Paperclip)</li>
        <li>Set the Company ID and Project ID for issue creation</li>
        <li>Configure agent routing to determine which agent handles each conversation</li>
      </ol>
    </div>
  );
}

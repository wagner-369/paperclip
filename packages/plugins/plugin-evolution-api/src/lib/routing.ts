/**
 * Agent routing — determines which Paperclip agent should handle an inbound message.
 */

import { normalisePhone } from "./normalise.js";

export interface AgentRoute {
  id: string;
  name: string;
  title: string;
}

export interface RoutingConfig {
  agents: Array<{ name: string; id: string; title?: string }>;
  defaultAgent: string;
  internalContacts: string[];
  internalDefaultAgent: string;
}

const DEFAULT_ROUTING: RoutingConfig = {
  agents: [],
  defaultAgent: "",
  internalContacts: [],
  internalDefaultAgent: "",
};

export function detectAgent(text: string | null, phone: string, config?: RoutingConfig): AgentRoute | null {
  const routing = config ?? DEFAULT_ROUTING;
  if (!routing.agents.length) return null;

  const agentMap = new Map(routing.agents.map((a) => [a.name.toLowerCase(), a]));

  // 1. Explicit mention in text
  if (text) {
    const lower = text.toLowerCase();
    for (const [keyword, agent] of agentMap) {
      if (lower.includes(keyword)) {
        return { id: agent.id, name: agent.name, title: agent.title ?? "" };
      }
    }
  }

  // 2. Internal contact → internal default
  const cleanPhone = normalisePhone(phone);
  if (routing.internalContacts.some((c) => cleanPhone.startsWith(c))) {
    const internal = agentMap.get(routing.internalDefaultAgent.toLowerCase());
    if (internal) {
      return { id: internal.id, name: internal.name, title: internal.title ?? "" };
    }
  }

  // 3. Default agent
  const defaultAgent = agentMap.get(routing.defaultAgent.toLowerCase());
  if (defaultAgent) {
    return { id: defaultAgent.id, name: defaultAgent.name, title: defaultAgent.title ?? "" };
  }

  // Fallback to first agent
  const first = routing.agents[0];
  return { id: first.id, name: first.name, title: first.title ?? "" };
}

export function getAgentName(agentId: string, config?: RoutingConfig): string {
  if (!config) return "Assistente";
  const agent = config.agents.find((a) => a.id === agentId);
  return agent?.name ?? "Assistente";
}

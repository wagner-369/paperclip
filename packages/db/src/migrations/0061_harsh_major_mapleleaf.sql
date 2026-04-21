CREATE TABLE "evolution_conversations" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"company_id" uuid NOT NULL,
	"instance_key" text NOT NULL,
	"phone" text NOT NULL,
	"contact_name" text,
	"channel" text DEFAULT 'whatsapp' NOT NULL,
	"issue_id" uuid,
	"assigned_agent_id" uuid,
	"status" text DEFAULT 'active' NOT NULL,
	"last_message_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "evolution_messages" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"conversation_id" uuid NOT NULL,
	"direction" text NOT NULL,
	"message_type" text NOT NULL,
	"content" text,
	"media_url" text,
	"media_mime_type" text,
	"evolution_msg_id" text,
	"issue_id" uuid,
	"issue_comment_id" uuid,
	"raw_payload" jsonb,
	"transcribed" boolean DEFAULT false NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "evolution_conversations" ADD CONSTRAINT "evolution_conversations_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "evolution_conversations" ADD CONSTRAINT "evolution_conversations_issue_id_issues_id_fk" FOREIGN KEY ("issue_id") REFERENCES "public"."issues"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "evolution_conversations" ADD CONSTRAINT "evolution_conversations_assigned_agent_id_agents_id_fk" FOREIGN KEY ("assigned_agent_id") REFERENCES "public"."agents"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "evolution_messages" ADD CONSTRAINT "evolution_messages_conversation_id_evolution_conversations_id_fk" FOREIGN KEY ("conversation_id") REFERENCES "public"."evolution_conversations"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "evo_conv_company_idx" ON "evolution_conversations" USING btree ("company_id");--> statement-breakpoint
CREATE UNIQUE INDEX "evo_conv_company_instance_phone_idx" ON "evolution_conversations" USING btree ("company_id","instance_key","phone");--> statement-breakpoint
CREATE INDEX "evo_conv_issue_idx" ON "evolution_conversations" USING btree ("issue_id");--> statement-breakpoint
CREATE INDEX "evo_conv_status_idx" ON "evolution_conversations" USING btree ("company_id","status");--> statement-breakpoint
CREATE INDEX "evo_msg_conv_created_at_idx" ON "evolution_messages" USING btree ("conversation_id","created_at");--> statement-breakpoint
CREATE UNIQUE INDEX "evo_msg_evolution_msg_id_idx" ON "evolution_messages" USING btree ("evolution_msg_id");
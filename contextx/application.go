package contextx

import (
	"context"
)

const (
	// Identity
	KeyUserID          = "user_id"
	KeyIdentityID      = "identity_id"
	KeyAnonymousUserID = "anonymous_user_id"
	KeyClientID        = "client_id"
	KeyAccountID       = "account_id"
	KeyTenantID        = "tenant_id"
	KeyOrganizationID  = "organization_id"
	KeyWorkspaceID     = "workspace_id"
	KeySessionID       = "session_id"

	// Request / correlation
	KeyRequestID      = "request_id"
	KeyCorrelationID  = "correlation_id"
	KeyIdempotencyKey = "idempotency_key"

	// Conversation / messaging
	KeyConversationID = "conversation_id"
	KeyMessageID      = "message_id"
	KeyThreadID       = "thread_id"
	KeyChannelID      = "channel_id"
	KeyEventID        = "event_id"
	KeyEventType      = "event_type"

	// Task / background processing
	KeyTaskID      = "task_id"
	KeyJobID       = "job_id"
	KeyBatchID     = "batch_id"
	KeyWorkflowID  = "workflow_id"
	KeyExecutionID = "execution_id"
	KeyAttempt     = "attempt"

	// Distributed tracing
	KeyTraceID = "trace_id"
	KeySpanID  = "span_id"

	// Network / client
	KeyClientIP  = "client_ip"
	KeyUserAgent = "user_agent"
	KeyDeviceID  = "device_id"
	KeyPlatform  = "platform"
	KeyLocale    = "locale"

	// Service / deployment
	KeyServiceName    = "service_name"
	KeyServiceVersion = "service_version"
	KeyEnvironment    = "environment"
	KeyRegion         = "region"
	KeyZone           = "zone"
	KeyInstanceID     = "instance_id"

	// Messaging / queue
	KeyTopic         = "topic"
	KeyQueue         = "queue"
	KeyPartition     = "partition"
	KeyConsumerGroup = "consumer_group"
	KeyDeliveryID    = "delivery_id"

	// Security / auth context
	KeySubjectID  = "subject_id"
	KeyActorID    = "actor_id"
	KeyRole       = "role"
	KeyScope      = "scope"
	KeyAuthMethod = "auth_method"

	// API / transport
	KeyOperation = "operation"
	KeyRoute     = "route"
	KeyMethod    = "method"
	KeyProtocol  = "protocol"

	// Generic extension
	KeyMetadata = "metadata"
)

// Identity

func SetUserID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyUserID, value)
}

func GetUserID(ctx context.Context) string {
	return GetString(ctx, KeyUserID)
}

func SetIdentityID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyIdentityID, value)
}

func GetIdentityID(ctx context.Context) string {
	return GetString(ctx, KeyIdentityID)
}

func SetAnonymousUserID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyAnonymousUserID, value)
}

func GetAnonymousUserID(ctx context.Context) string {
	return GetString(ctx, KeyAnonymousUserID)
}

func SetClientID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyClientID, value)
}

func GetClientID(ctx context.Context) string {
	return GetString(ctx, KeyClientID)
}

func SetAccountID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyAccountID, value)
}

func GetAccountID(ctx context.Context) string {
	return GetString(ctx, KeyAccountID)
}

func SetTenantID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyTenantID, value)
}

func GetTenantID(ctx context.Context) string {
	return GetString(ctx, KeyTenantID)
}

func SetOrganizationID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyOrganizationID, value)
}

func GetOrganizationID(ctx context.Context) string {
	return GetString(ctx, KeyOrganizationID)
}

func SetWorkspaceID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyWorkspaceID, value)
}

func GetWorkspaceID(ctx context.Context) string {
	return GetString(ctx, KeyWorkspaceID)
}

func SetSessionID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeySessionID, value)
}

func GetSessionID(ctx context.Context) string {
	return GetString(ctx, KeySessionID)
}

// Request / correlation

func SetRequestID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyRequestID, value)
}

func GetRequestID(ctx context.Context) string {
	return GetString(ctx, KeyRequestID)
}

func SetCorrelationID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyCorrelationID, value)
}

func GetCorrelationID(ctx context.Context) string {
	return GetString(ctx, KeyCorrelationID)
}

func SetIdempotencyKey(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyIdempotencyKey, value)
}

func GetIdempotencyKey(ctx context.Context) string {
	return GetString(ctx, KeyIdempotencyKey)
}

// Conversation / messaging

func SetConversationID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyConversationID, value)
}

func GetConversationID(ctx context.Context) string {
	return GetString(ctx, KeyConversationID)
}

func SetMessageID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyMessageID, value)
}

func GetMessageID(ctx context.Context) string {
	return GetString(ctx, KeyMessageID)
}

func SetThreadID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyThreadID, value)
}

func GetThreadID(ctx context.Context) string {
	return GetString(ctx, KeyThreadID)
}

func SetChannelID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyChannelID, value)
}

func GetChannelID(ctx context.Context) string {
	return GetString(ctx, KeyChannelID)
}

func SetEventID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyEventID, value)
}

func GetEventID(ctx context.Context) string {
	return GetString(ctx, KeyEventID)
}

func SetEventType(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyEventType, value)
}

func GetEventType(ctx context.Context) string {
	return GetString(ctx, KeyEventType)
}

// Task / background processing

func SetTaskID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyTaskID, value)
}

func GetTaskID(ctx context.Context) string {
	return GetString(ctx, KeyTaskID)
}

func SetJobID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyJobID, value)
}

func GetJobID(ctx context.Context) string {
	return GetString(ctx, KeyJobID)
}

func SetBatchID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyBatchID, value)
}

func GetBatchID(ctx context.Context) string {
	return GetString(ctx, KeyBatchID)
}

func SetWorkflowID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyWorkflowID, value)
}

func GetWorkflowID(ctx context.Context) string {
	return GetString(ctx, KeyWorkflowID)
}

func SetExecutionID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyExecutionID, value)
}

func GetExecutionID(ctx context.Context) string {
	return GetString(ctx, KeyExecutionID)
}

func SetAttempt(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyAttempt, value)
}

func GetAttempt(ctx context.Context) string {
	return GetString(ctx, KeyAttempt)
}

// Distributed tracing

func SetTraceID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyTraceID, value)
}

func GetTraceID(ctx context.Context) string {
	return GetString(ctx, KeyTraceID)
}

func SetSpanID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeySpanID, value)
}

func GetSpanID(ctx context.Context) string {
	return GetString(ctx, KeySpanID)
}

// Network / client

func SetClientIP(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyClientIP, value)
}

func GetClientIP(ctx context.Context) string {
	return GetString(ctx, KeyClientIP)
}

func SetUserAgent(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyUserAgent, value)
}

func GetUserAgent(ctx context.Context) string {
	return GetString(ctx, KeyUserAgent)
}

func SetDeviceID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyDeviceID, value)
}

func GetDeviceID(ctx context.Context) string {
	return GetString(ctx, KeyDeviceID)
}

func SetPlatform(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyPlatform, value)
}

func GetPlatform(ctx context.Context) string {
	return GetString(ctx, KeyPlatform)
}

func SetLocale(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyLocale, value)
}

func GetLocale(ctx context.Context) string {
	return GetString(ctx, KeyLocale)
}

// Service / deployment

func SetServiceName(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyServiceName, value)
}

func GetServiceName(ctx context.Context) string {
	return GetString(ctx, KeyServiceName)
}

func SetServiceVersion(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyServiceVersion, value)
}

func GetServiceVersion(ctx context.Context) string {
	return GetString(ctx, KeyServiceVersion)
}

func SetEnvironment(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyEnvironment, value)
}

func GetEnvironment(ctx context.Context) string {
	return GetString(ctx, KeyEnvironment)
}

func SetRegion(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyRegion, value)
}

func GetRegion(ctx context.Context) string {
	return GetString(ctx, KeyRegion)
}

func SetZone(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyZone, value)
}

func GetZone(ctx context.Context) string {
	return GetString(ctx, KeyZone)
}

func SetInstanceID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyInstanceID, value)
}

func GetInstanceID(ctx context.Context) string {
	return GetString(ctx, KeyInstanceID)
}

// Messaging / queue

func SetTopic(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyTopic, value)
}

func GetTopic(ctx context.Context) string {
	return GetString(ctx, KeyTopic)
}

func SetQueue(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyQueue, value)
}

func GetQueue(ctx context.Context) string {
	return GetString(ctx, KeyQueue)
}

func SetPartition(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyPartition, value)
}

func GetPartition(ctx context.Context) string {
	return GetString(ctx, KeyPartition)
}

func SetConsumerGroup(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyConsumerGroup, value)
}

func GetConsumerGroup(ctx context.Context) string {
	return GetString(ctx, KeyConsumerGroup)
}

func SetDeliveryID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyDeliveryID, value)
}

func GetDeliveryID(ctx context.Context) string {
	return GetString(ctx, KeyDeliveryID)
}

// Security / auth context

func SetSubjectID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeySubjectID, value)
}

func GetSubjectID(ctx context.Context) string {
	return GetString(ctx, KeySubjectID)
}

func SetActorID(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyActorID, value)
}

func GetActorID(ctx context.Context) string {
	return GetString(ctx, KeyActorID)
}

func SetRole(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyRole, value)
}

func GetRole(ctx context.Context) string {
	return GetString(ctx, KeyRole)
}

func SetScope(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyScope, value)
}

func GetScope(ctx context.Context) string {
	return GetString(ctx, KeyScope)
}

func SetAuthMethod(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyAuthMethod, value)
}

func GetAuthMethod(ctx context.Context) string {
	return GetString(ctx, KeyAuthMethod)
}

// API / transport

func SetOperation(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyOperation, value)
}

func GetOperation(ctx context.Context) string {
	return GetString(ctx, KeyOperation)
}

func SetRoute(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyRoute, value)
}

func GetRoute(ctx context.Context) string {
	return GetString(ctx, KeyRoute)
}

func SetMethod(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyMethod, value)
}

func GetMethod(ctx context.Context) string {
	return GetString(ctx, KeyMethod)
}

func SetProtocol(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyProtocol, value)
}

func GetProtocol(ctx context.Context) string {
	return GetString(ctx, KeyProtocol)
}

// Generic extension

func SetMetadata(ctx context.Context, value string) context.Context {
	return SetString(ctx, KeyMetadata, value)
}

func GetMetadata(ctx context.Context) string {
	return GetString(ctx, KeyMetadata)
}

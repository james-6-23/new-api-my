package dto

type ChannelSettings struct {
	ForceFormat            bool   `json:"force_format,omitempty"`
	ThinkingToContent      bool   `json:"thinking_to_content,omitempty"`
	Proxy                  string `json:"proxy"`
	PassThroughBodyEnabled bool   `json:"pass_through_body_enabled,omitempty"`
	SystemPrompt           string `json:"system_prompt,omitempty"`
	SystemPromptOverride   bool   `json:"system_prompt_override,omitempty"`
}

type VertexKeyType string

const (
	VertexKeyTypeJSON   VertexKeyType = "json"
	VertexKeyTypeAPIKey VertexKeyType = "api_key"
)

type AwsKeyType string

const (
	AwsKeyTypeAKSK   AwsKeyType = "ak_sk" // 默认
	AwsKeyTypeApiKey AwsKeyType = "api_key"
)

type ChannelOtherSettings struct {
	AzureResponsesVersion                 string        `json:"azure_responses_version,omitempty"`
	VertexKeyType                         VertexKeyType `json:"vertex_key_type,omitempty"` // "json" or "api_key"
	OpenRouterEnterprise                  *bool         `json:"openrouter_enterprise,omitempty"`
	ClaudeBetaQuery                       bool          `json:"claude_beta_query,omitempty"`         // Claude 渠道是否强制追加 ?beta=true
	AllowServiceTier                      bool          `json:"allow_service_tier,omitempty"`        // 是否允许 service_tier 透传（默认过滤以避免额外计费）
	AllowInferenceGeo                     bool          `json:"allow_inference_geo,omitempty"`       // 是否允许 inference_geo 透传（仅 Claude，默认过滤以满足数据驻留合规
	AllowSpeed                            bool          `json:"allow_speed,omitempty"`               // 是否允许 speed 透传（仅 Claude，默认过滤以避免意外切换推理速度模式）
	AllowSafetyIdentifier                 bool          `json:"allow_safety_identifier,omitempty"`   // 是否允许 safety_identifier 透传（默认过滤以保护用户隐私）
	DisableStore                          bool          `json:"disable_store,omitempty"`             // 是否禁用 store 透传（默认允许透传，禁用后可能导致 Codex 无法使用）
	AllowIncludeObfuscation               bool          `json:"allow_include_obfuscation,omitempty"` // 是否允许 stream_options.include_obfuscation 透传（默认过滤以避免关闭流混淆保护）
	ConversationLogEnabled                bool          `json:"conversation_log_enabled,omitempty"`  // Root-only: capture full provider-facing payloads for strict traj export
	AwsKeyType                            AwsKeyType    `json:"aws_key_type,omitempty"`
	UpstreamModelUpdateCheckEnabled       bool          `json:"upstream_model_update_check_enabled,omitempty"`        // 是否检测上游模型更新
	UpstreamModelUpdateAutoSyncEnabled    bool          `json:"upstream_model_update_auto_sync_enabled,omitempty"`    // 是否自动同步上游模型更新
	UpstreamModelUpdateLastCheckTime      int64         `json:"upstream_model_update_last_check_time,omitempty"`      // 上次检测时间
	UpstreamModelUpdateLastDetectedModels []string      `json:"upstream_model_update_last_detected_models,omitempty"` // 上次检测到的可加入模型
	UpstreamModelUpdateLastRemovedModels  []string      `json:"upstream_model_update_last_removed_models,omitempty"`  // 上次检测到的可删除模型
	UpstreamModelUpdateIgnoredModels      []string      `json:"upstream_model_update_ignored_models,omitempty"`       // 手动忽略的模型

	// 客户端限制：限制只有特定客户端（如 Claude Code CLI）才能使用该渠道
	ClientRestrictionEnabled bool                  `json:"client_restriction_enabled,omitempty"` // 是否开启客户端限制
	ClientRestrictionMode    ClientRestrictionMode `json:"client_restriction_mode,omitempty"`    // "allow"=白名单（仅列出的客户端可用）/ "block"=黑名单（列出的客户端禁用）
	ClientRestrictionClients []string              `json:"client_restriction_clients,omitempty"` // 客户端标识列表，如 "claude-code"、"codex-cli"、"gemini-cli" 或自定义 UA 关键词

	// Claude Code 防伪装识别参数（仅对内置 "claude-code" 标识生效）。
	// 使用指针以区分「未配置」(nil，回退默认值) 与「显式设置」；避免非指针 0 值被当作阈值=0 而放行一切。
	ClientRestrictionClaudeCodeMinScore      *int  `json:"client_restriction_claude_code_min_score,omitempty"`      // 加权打分阈值，nil 时回退默认 4；范围约 1~13
	ClientRestrictionClaudeCodeRequireStrong *bool `json:"client_restriction_claude_code_require_strong,omitempty"` // 是否必须命中强信号(system prompt / x-stainless 全套头)，nil 时回退默认 true
}

type ClientRestrictionMode string

const (
	ClientRestrictionModeAllow ClientRestrictionMode = "allow" // 白名单模式
	ClientRestrictionModeBlock ClientRestrictionMode = "block" // 黑名单模式
)

func (s *ChannelOtherSettings) IsOpenRouterEnterprise() bool {
	if s == nil || s.OpenRouterEnterprise == nil {
		return false
	}
	return *s.OpenRouterEnterprise
}

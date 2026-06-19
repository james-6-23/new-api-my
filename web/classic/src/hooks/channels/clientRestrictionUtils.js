/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/

const CLIENT_RESTRICTION_PRESET_LABELS = {
  'claude-code': 'Claude Code',
  codex: 'Codex',
  'codex-cli': 'Codex',
  'codex-tui': 'Codex',
  'codex-vscode': 'Codex',
  'codex-desktop': 'Codex',
  'gemini-cli': 'Gemini CLI',
  'factory-cli': 'Factory CLI',
};

export const formatClientRestrictionClientLabel = (client) => {
  const key = String(client || '').trim();
  if (!key) {
    return '';
  }
  return CLIENT_RESTRICTION_PRESET_LABELS[key] || key;
};

export const parseClientRestrictionMeta = (settings) => {
  let parsed = null;
  if (settings && typeof settings === 'object') {
    parsed = settings;
  } else if (typeof settings === 'string') {
    try {
      parsed = JSON.parse(settings);
    } catch (error) {
      parsed = null;
    }
  }

  if (!parsed || typeof parsed !== 'object') {
    return {
      enabled: false,
      mode: 'allow',
      clients: [],
      clientCount: 0,
    };
  }

  const clients = Array.isArray(parsed.client_restriction_clients)
    ? parsed.client_restriction_clients
        .map((item) => String(item || '').trim())
        .filter(Boolean)
    : [];

  return {
    enabled: parsed.client_restriction_enabled === true,
    mode: parsed.client_restriction_mode === 'block' ? 'block' : 'allow',
    clients,
    clientCount: clients.length,
  };
};

export const getClientRestrictionBadgeText = (meta, t) => {
  if (!meta?.enabled) {
    return '';
  }
  const count = meta.clientCount;
  if (meta.mode === 'block') {
    return t('客户端限制 · 黑名单({{count}})', { count });
  }
  return t('客户端限制 · 白名单({{count}})', { count });
};

export const getClientRestrictionTooltip = (meta, t) => {
  if (!meta?.enabled) {
    return '';
  }
  const modeLabel =
    meta.mode === 'block' ? t('黑名单模式') : t('白名单模式');
  const clientLines = meta.clients.map((client) =>
    formatClientRestrictionClientLabel(client),
  );
  const clientsText =
    clientLines.length > 0 ? clientLines.join('、') : t('（未配置客户端）');
  return `${modeLabel}\n${t('客户端')}：${clientsText}`;
};
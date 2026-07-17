// ============================================================
// Bot de atendimento AdGain — WhatsApp Cloud API (Meta)
// Fase B: webhook + menu interativo + respostas da base de conhecimento.
//
// Env vars (Render):
//   WHATSAPP_TOKEN         token de acesso da Cloud API (temporário 24h em dev;
//                          trocar por token permanente de usuário de sistema)
//   WHATSAPP_PHONE_ID      Phone Number ID do número remetente (teste: 1138772532662571)
//   WHATSAPP_VERIFY_TOKEN  segredo do handshake GET do webhook (definido por nós)
//   ADMIN_WHATSAPP         (opcional) número que recebe aviso de "falar com humano",
//                          formato E.164 sem '+' (ex.: 5565999757621)
//
// Registro na Meta (app AdGain Atendimento > WhatsApp > Configuração):
//   Callback URL: https://adgain-api.onrender.com/api/whatsapp/webhook
//   Verify token: valor de WHATSAPP_VERIFY_TOKEN
//   Campos assinados: messages
// ============================================================

const GRAPH_VERSION = 'v23.0';
const { getDb } = require('./firebase');
const Anthropic = require('@anthropic-ai/sdk');
const {
  PLAN_NAMES,
  PRIORITY_PLANS,
  getPlanosText,
  buildSystemPrompt,
  CANNED,
  cannedAnswer,
} = require('./bot-knowledge');

let anthropicClient = null;
function getAnthropic() {
  if (!anthropicClient && process.env.ANTHROPIC_API_KEY) {
    anthropicClient = new Anthropic();
  }
  return anthropicClient;
}

// ------------------------------------------------------------
// Base de conhecimento (Fase B: estática — espelha as respostas
// rápidas do WhatsApp Business; Fase C: preços vivos do Firestore)
// ------------------------------------------------------------
const KB = {
  saudacao:
    'Olá! 🌱 Você fala com o assistente da *AdGain* — sua terra com o valor certo.\n\n' +
    'Escolha uma opção no menu abaixo, ou escreva sua pergunta.',

  planos:
    '🌱 *Planos AdGain* (por mês):\n\n' +
    '▫️ *Gratuito* — R$ 0 (3 fotos por anúncio)\n' +
    '▫️ *Essencial* — R$ 12,00 (30 créditos/mês, 5 fotos)\n' +
    '▫️ *Profissional* — R$ 19,90 (100 créditos/mês, 10 fotos, estatísticas)\n' +
    '▫️ *Empresarial* — R$ 49,90 (400 créditos/mês, 15 fotos, analytics completo)\n' +
    '▫️ *Premium* — R$ 99,90 (800 créditos/mês, 15 fotos, analytics completo, suporte VIP)\n\n' +
    'Compare e assine em: www.adgain.com.br/plans',

  anunciar: CANNED.anunciar,

  analise:
    CANNED.analise +
    '\n\nSe sua análise apresentou algum problema, digite *humano* que nossa equipe verifica para você.',

  humano:
    '👤 Certo! Encaminhei sua conversa para a nossa equipe.\n\n' +
    'Um atendente humano vai te responder por aqui o mais rápido possível ' +
    '(horário comercial: seg–sex, 8h–18h).\n\n' +
    'Enquanto isso, pode adiantar sua dúvida em uma mensagem que ele já chega sabendo do assunto. 👍',

  foraDoEscopo:
    'Boa pergunta! Essa eu prefiro confirmar com a nossa equipe para não te passar informação errada. 🙏\n\n' +
    'Digite *humano* para falar com um atendente, ou escolha uma opção do menu digitando *menu*.',
};

// ------------------------------------------------------------
// Estado em memória (suficiente para 1 instância no Render)
// ------------------------------------------------------------
const processedMsgs = new Map(); // msgId -> timestamp (dedup de retries do webhook)
const humanMode = new Map(); // waId -> timestamp (bot silencia após pedir humano)
const lastStatuses = []; // últimos statuses de entrega (diagnóstico via /api/whatsapp/status)
const userCache = new Map(); // waId -> { user: {uid,nome,plano}|null, ts }
const chatHistory = new Map(); // waId -> { msgs: [{role,content}], ts } (contexto do Claude)
const PROCESSED_TTL_MS = 10 * 60 * 1000;
const HUMAN_MODE_TTL_MS = 12 * 60 * 60 * 1000;
const USER_CACHE_TTL_MS = 10 * 60 * 1000;
const CHAT_HISTORY_TTL_MS = 2 * 60 * 60 * 1000;

// Humanos do suporte: lista separada por vírgula (E.164 sem '+').
// Padrão: linha empresarial (Nilton) + Miguel; sobrescrevível via ADMIN_WHATSAPP.
const DEFAULT_ADMINS = '5565999988127,5565998180637';
function adminNumbers() {
  return (process.env.ADMIN_WHATSAPP || DEFAULT_ADMINS)
    .split(',')
    .map((n) => n.trim())
    .filter(Boolean);
}

// Forma canônica p/ comparar números BR: o wa_id pode vir sem o 9º dígito
// (ex.: 556599998127 vs 5565999988127) — removemos o 9 extra ao comparar.
function canonNumber(n) {
  const d = String(n || '').replace(/\D+/g, '');
  return d.length === 13 && d.startsWith('55') && d[4] === '9' ? d.slice(0, 4) + d.slice(5) : d;
}
function isAdmin(waId) {
  const c = canonNumber(waId);
  return adminNumbers().some((a) => canonNumber(a) === c);
}

// ------------------------------------------------------------
// Fila de alertas de atendimento humano (Firestore) — quando a janela de 24h
// do admin está fechada, o alerta falha (131047); guardamos e entregamos na
// próxima mensagem que o admin mandar ao bot (entrega grátis, sem template).
// ------------------------------------------------------------
const alertsByWamid = new Map(); // wamid do alerta enviado -> dados (p/ detectar falha async)
const ALERT_TRACK_TTL_MS = 60 * 60 * 1000;

async function queuePendingAlert(alerta) {
  try {
    const db = getDb();
    if (!db) return;
    const docId = `${alerta.cliente}-${String(alerta.criado || '').replace(/[:.]/g, '-')}`;
    await db.collection('whatsapp_pending_alerts').doc(docId).set(
      {
        cliente: alerta.cliente,
        nome: alerta.nome || null,
        plano: alerta.plano || null,
        prioritario: !!alerta.prioritario,
        criado: alerta.criado,
      },
      { merge: true }
    );
    console.log('[wa-bot] Alerta guardado na fila (janela do admin fechada):', alerta.cliente);
  } catch (err) {
    console.error('[wa-bot] Falha ao guardar alerta na fila:', err.message);
  }
}

async function flushPendingAlerts(admin) {
  try {
    const db = getDb();
    if (!db) return;
    const snap = await db
      .collection('whatsapp_pending_alerts')
      .orderBy('criado')
      .limit(20)
      .get();
    if (snap.empty) return;
    const linhas = snap.docs.map((d) => {
      const a = d.data();
      let quando = '';
      try {
        quando = new Date(a.criado).toLocaleString('pt-BR', {
          timeZone: 'America/Cuiaba',
          day: '2-digit',
          month: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
        });
      } catch (_) {}
      return `• ${a.nome || 'sem nome'} — ${a.plano || 'visitante'}${a.prioritario ? ' ⭐' : ''} (wa.me/${a.cliente})${quando ? ` — ${quando}` : ''}`;
    });
    await sendText(
      admin,
      `⏳ *Enquanto você esteve fora:* ${snap.size} pedido(s) de atendimento humano\n\n${linhas.join('\n')}\n\nToque no link para responder o cliente.`
    );
    const batch = db.batch();
    snap.docs.forEach((d) => batch.delete(d.ref));
    await batch.commit();
    console.log('[wa-bot] Fila de alertas entregue para', admin, `(${snap.size})`);
  } catch (err) {
    console.error('[wa-bot] Falha ao entregar fila de alertas:', err.message);
  }
}

// ------------------------------------------------------------
// Cliente AdGain: identificação pelo número (users.phoneDigits)
// e plano efetivo (campos planos primeiro; profile.subscription é legado)
// ------------------------------------------------------------
async function lookupUser(waId) {
  const hit = userCache.get(waId);
  if (hit && Date.now() - hit.ts < USER_CACHE_TTL_MS) return hit.user;
  let user = null;
  try {
    const db = getDb();
    if (db) {
      const snap = await db
        .collection('users')
        .where('phoneDigits', 'array-contains', waId)
        .limit(1)
        .get();
      if (!snap.empty) {
        const d = snap.docs[0].data() || {};
        let plano = 'gratuito';
        if (d.subscriptionPlan && (!d.subscriptionStatus || d.subscriptionStatus === 'active')) {
          plano = d.subscriptionPlan;
        } else if (d.profile && d.profile.subscription && d.profile.subscription.planType) {
          plano = d.profile.subscription.planType;
        }
        user = {
          uid: snap.docs[0].id,
          nome: d.displayName || null,
          plano,
        };
      }
    }
  } catch (err) {
    console.error('[wa-bot] lookup de usuário falhou:', err.message);
  }
  userCache.set(waId, { user, ts: Date.now() });
  return user;
}

// Preços vivos: getPlanosText importado de bot-knowledge.js

function _sweep(map, ttl) {
  const now = Date.now();
  for (const [k, t] of map) if (now - t > ttl) map.delete(k);
}
setInterval(() => {
  _sweep(processedMsgs, PROCESSED_TTL_MS);
  _sweep(humanMode, HUMAN_MODE_TTL_MS);
  const now = Date.now();
  for (const [k, v] of chatHistory) if (now - v.ts > CHAT_HISTORY_TTL_MS) chatHistory.delete(k);
  for (const [k, v] of alertsByWamid) if (now - v.ts > ALERT_TRACK_TTL_MS) alertsByWamid.delete(k);
}, 5 * 60 * 1000).unref();

// ------------------------------------------------------------
// Fase C2: Claude responde perguntas livres (fora do menu)
// ------------------------------------------------------------
async function askClaude(from, texto, user) {
  const client = getAnthropic();
  if (!client) return null;
  try {
    const hist = (chatHistory.get(from) && chatHistory.get(from).msgs) || [];
    const resp = await client.messages.create({
      model: 'claude-haiku-4-5',
      max_tokens: 400,
      system: await buildSystemPrompt('whatsapp', user),
      messages: [...hist, { role: 'user', content: texto }],
    });
    const answer = resp.content
      .filter((b) => b.type === 'text')
      .map((b) => b.text)
      .join('')
      .trim();
    if (!answer) return null;
    chatHistory.set(from, {
      msgs: [...hist, { role: 'user', content: texto }, { role: 'assistant', content: answer }].slice(-8),
      ts: Date.now(),
    });
    return answer;
  } catch (err) {
    console.error('[wa-bot] Claude falhou:', err.message);
    return null;
  }
}

// ------------------------------------------------------------
// Envio via Graph API
// ------------------------------------------------------------
async function waSend(payload) {
  const phoneId = process.env.WHATSAPP_PHONE_ID;
  const token = process.env.WHATSAPP_TOKEN;
  if (!phoneId || !token) {
    console.warn('[wa-bot] WHATSAPP_PHONE_ID/WHATSAPP_TOKEN ausentes — envio ignorado');
    return null;
  }
  const resp = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/${phoneId}/messages`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ messaging_product: 'whatsapp', ...payload }),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    console.error('[wa-bot] Falha no envio:', resp.status, JSON.stringify(data));
  } else {
    console.log('[wa-bot] Enviado para', payload.to, '-', (data.messages && data.messages[0] && data.messages[0].id) || 'ok');
  }
  return data;
}

function sendText(to, body) {
  return waSend({ to, type: 'text', text: { body, preview_url: false } });
}

function sendMenu(to, user) {
  let saudacao = KB.saudacao;
  if (user) {
    const primeiroNome = (user.nome || '').split(' ')[0];
    const plano = PLAN_NAMES[user.plano] || 'Gratuito';
    saudacao =
      `Olá${primeiroNome ? `, *${primeiroNome}*` : ''}! 🌱 Você fala com o assistente da *AdGain*.\n\n` +
      `Vi aqui que você é cliente do plano *${plano}*. ` +
      'Escolha uma opção no menu abaixo, ou escreva sua pergunta.';
  }
  return waSend({
    to,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: { text: saudacao },
      footer: { text: 'AdGain • adgain.com.br' },
      action: {
        button: 'Ver opções',
        sections: [
          {
            title: 'Atendimento AdGain',
            rows: [
              { id: 'op_planos', title: '1️⃣ Planos e créditos', description: 'Preços, créditos mensais e assinatura' },
              { id: 'op_anunciar', title: '2️⃣ Como anunciar', description: 'Passo a passo para publicar sua terra' },
              { id: 'op_analise', title: '3️⃣ Análise técnica', description: 'O raio-X da sua propriedade' },
              { id: 'op_ganhos', title: '4️⃣ Ganhos ao anunciar', description: 'Recompensas e saque via Pix' },
              { id: 'op_niveis', title: '5️⃣ Níveis e validação', description: 'Hierarquia de ganhos por perfil' },
              { id: 'op_humano', title: '6️⃣ Falar com humano', description: 'Equipe AdGain — exclusivo para assinantes' },
            ],
          },
        ],
      },
    },
  });
}

// ------------------------------------------------------------
// Roteamento de mensagens recebidas
// ------------------------------------------------------------
const OPTION_HANDLERS = {
  op_planos: async (to) => sendText(to, await getPlanosText()),
  op_anunciar: (to) => sendText(to, KB.anunciar),
  op_analise: (to) => sendText(to, KB.analise),
  op_ganhos: async (to) => sendText(to, await cannedAnswer('ganhos')),
  op_niveis: async (to) => sendText(to, await cannedAnswer('niveis')),
  op_humano: async (to, ctx) => {
    const user = ctx && ctx.user;
    // Atendimento humano: benefício exclusivo de plano pago ativo
    const pago = user && user.plano && user.plano !== 'gratuito';
    if (!pago) {
      return sendText(
        to,
        '👤 O atendimento com a nossa equipe é um benefício dos *planos pagos* AdGain.\n\n' +
          'Mas não te deixo na mão: manda sua dúvida aqui que eu resolvo com você! 💪\n\n' +
          'E se quiser contar com a equipe (e muito mais), conheça os planos: www.adgain.com.br/plans'
      );
    }
    humanMode.set(to, Date.now());
    const prioritario = PRIORITY_PLANS.includes(user.plano);
    if (prioritario) {
      const dedicado = user.plano === 'premium' || user.plano === 'enterprise';
      await sendText(
        to,
        `👤 Certo! Sua conversa entrou na *fila prioritária* da nossa equipe${dedicado ? ' (atendimento dedicado Premium)' : ''}.\n\n` +
          'Um atendente vai te responder por aqui o quanto antes (horário comercial: seg–sex, 8h–18h).\n\n' +
          'Pode adiantar sua dúvida em uma mensagem que ele já chega sabendo do assunto. 👍'
      );
    } else {
      await sendText(to, KB.humano);
    }
    const nome = (user && user.nome) || (ctx && ctx.profileName) || 'sem nome';
    const plano = user ? PLAN_NAMES[user.plano] || user.plano : 'visitante';
    const alerta = { cliente: to, nome, plano, prioritario, criado: new Date().toISOString() };
    for (const admin of adminNumbers()) {
      const r = await sendText(
        admin,
        `🔔 *Pedido de atendimento humano*${prioritario ? ' ⭐ PRIORITÁRIO' : ''}\n` +
          `Cliente: ${nome} — plano ${plano} (wa.me/${to})\n` +
          'Responda pelo WhatsApp Business.'
      );
      const wamid = r && r.messages && r.messages[0] && r.messages[0].id;
      if (wamid) {
        // Falha de entrega chega async via status webhook — rastreamos p/ enfileirar
        alertsByWamid.set(wamid, { ...alerta, ts: Date.now() });
      } else {
        // Falha síncrona no envio: enfileira direto
        await queuePendingAlert(alerta);
      }
    }
  },
};

// Atalhos digitados (1-4, palavras-chave)
function matchShortcut(text) {
  const t = (text || '').trim().toLowerCase();
  if (/^(1|planos?|creditos?|créditos?|preços?|precos?)$/.test(t)) return 'op_planos';
  if (/^(2|anunciar|anuncio|anúncio|vender)$/.test(t)) return 'op_anunciar';
  if (/^(3|analise|análise|relatorio|relatório)$/.test(t)) return 'op_analise';
  if (/^(4|ganhos?|recompensas?|saque)$/.test(t)) return 'op_ganhos';
  if (/^(5|niveis|níveis|nivel|nível|validacao|validação)$/.test(t)) return 'op_niveis';
  if (/^(6|humano|atendente|pessoa|suporte)$/.test(t)) return 'op_humano';
  if (/^(menu|oi|ola|olá|bom dia|boa tarde|boa noite|inicio|início|start)$/.test(t)) return 'menu';
  return null;
}

async function handleIncomingMessage(msg, contacts) {
  const from = msg.from; // wa_id do cliente (E.164 sem '+')
  const profileName = contacts && contacts[0] && contacts[0].profile && contacts[0].profile.name;

  // Mensagem de um humano do suporte: primeiro entrega alertas represados
  // (a janela de 24h acabou de abrir), depois segue o fluxo normal
  if (isAdmin(from)) {
    await flushPendingAlerts(from);
  }

  // Cliente em modo humano: bot fica em silêncio (a equipe responde pelo app)
  if (humanMode.has(from)) {
    // "menu" reativa o bot
    const t = msg.type === 'text' ? (msg.text.body || '').trim().toLowerCase() : '';
    if (t === 'menu') {
      humanMode.delete(from);
      return sendMenu(from, await lookupUser(from));
    }
    return;
  }

  const user = await lookupUser(from);
  const ctx = { profileName, user };

  // Resposta de menu interativo (list)
  if (msg.type === 'interactive') {
    const replyId =
      (msg.interactive.list_reply && msg.interactive.list_reply.id) ||
      (msg.interactive.button_reply && msg.interactive.button_reply.id);
    const handler = OPTION_HANDLERS[replyId];
    if (handler) return handler(from, ctx);
    return sendMenu(from, user);
  }

  // Texto digitado
  if (msg.type === 'text') {
    const shortcut = matchShortcut(msg.text.body);
    if (shortcut === 'menu') return sendMenu(from, user);
    if (shortcut && OPTION_HANDLERS[shortcut]) return OPTION_HANDLERS[shortcut](from, ctx);
    // Fase C2: pergunta livre -> Claude com a base de conhecimento
    const resposta = await askClaude(from, msg.text.body, user);
    return sendText(from, resposta || KB.foraDoEscopo);
  }

  // Áudio, imagem, sticker etc.: orienta para o menu
  return sendText(from, 'Por enquanto eu entendo melhor mensagens de texto. 🙂 Digite *menu* para ver as opções.');
}

// ------------------------------------------------------------
// Registro das rotas
// ------------------------------------------------------------
module.exports = function registerWhatsAppBot(app) {
  // Handshake de verificação da Meta (uma vez, ao cadastrar o webhook)
  app.get('/api/whatsapp/webhook', (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];
    if (mode === 'subscribe' && token && token === process.env.WHATSAPP_VERIFY_TOKEN) {
      console.log('[wa-bot] Webhook verificado pela Meta');
      return res.status(200).send(challenge);
    }
    return res.sendStatus(403);
  });

  // Recepção de eventos (mensagens, statuses)
  app.post('/api/whatsapp/webhook', (req, res) => {
    // A Meta exige 200 rápido; processamento segue async
    res.sendStatus(200);

    try {
      const entries = (req.body && req.body.entry) || [];
      // Diagnóstico: registra todo evento recebido (recortado)
      console.log('[wa-bot] Webhook POST:', JSON.stringify(req.body || {}).slice(0, 600));
      for (const entry of entries) {
        for (const change of entry.changes || []) {
          const value = change.value || {};
          for (const msg of value.messages || []) {
            if (processedMsgs.has(msg.id)) continue; // retry da Meta
            processedMsgs.set(msg.id, Date.now());
            handleIncomingMessage(msg, value.contacts).catch((err) =>
              console.error('[wa-bot] Erro ao processar mensagem:', err)
            );
          }
          // Statuses de entrega (sent/delivered/read/failed) — guardados p/ diagnóstico
          for (const st of value.statuses || []) {
            // Alerta p/ admin não entregue (janela de 24h fechada)? Vai p/ a fila.
            if (st.status === 'failed' && alertsByWamid.has(st.id)) {
              queuePendingAlert(alertsByWamid.get(st.id)).catch(() => {});
              alertsByWamid.delete(st.id);
            }
            lastStatuses.push({
              quando: new Date().toISOString(),
              para: st.recipient_id,
              status: st.status,
              erro: (st.errors && st.errors[0] && st.errors[0].code) || null,
            });
            if (lastStatuses.length > 20) lastStatuses.shift();
          }
        }
      }
    } catch (err) {
      console.error('[wa-bot] Erro no webhook:', err);
    }
  });

  // Utilitário: assina o app na WABA (necessário para receber mensagens; a Meta
  // nem sempre cria esse vínculo sozinha). Protegido pelo verify token.
  // GET  /api/whatsapp/subscribe?token=...        -> consulta assinatura atual
  // GET  /api/whatsapp/subscribe?token=...&do=1   -> cria a assinatura
  app.get('/api/whatsapp/subscribe', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    try {
      const waba = process.env.WHATSAPP_WABA_ID || '1011685214925033';
      const url = `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/subscribed_apps`;
      const opts = {
        method: req.query.do ? 'POST' : 'GET',
        headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}` },
      };
      const r = await fetch(url, opts);
      const d = await r.json().catch(() => ({}));
      console.log('[wa-bot] subscribed_apps', opts.method, r.status, JSON.stringify(d).slice(0, 300));
      res.json({ metodo: opts.method, status: r.status, resposta: d });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Diagnóstico: estado do número e da WABA na Meta (protegido pelo verify token)
  app.get('/api/whatsapp/diag', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    try {
      const headers = { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}` };
      const waba = process.env.WHATSAPP_WABA_ID || '1011685214925033';
      const [numero, conta] = await Promise.all([
        fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${process.env.WHATSAPP_PHONE_ID}?fields=display_phone_number,verified_name,name_status,code_verification_status,quality_rating,platform_type,status`,
          { headers }
        ).then((r) => r.json()),
        fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${waba}?fields=name,account_review_status,business_verification_status,ownership_type,country`,
          { headers }
        ).then((r) => r.json()),
      ]);
      res.json({ numero, conta });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Registro do número na plataforma Cloud API (uma vez por número).
  // GET /api/whatsapp/register?token=...&pin=NNNNNN
  app.get('/api/whatsapp/register', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    if (!/^\d{6}$/.test(req.query.pin || '')) {
      return res.status(400).json({ error: 'pin de 6 dígitos obrigatório' });
    }
    try {
      const r = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${process.env.WHATSAPP_PHONE_ID}/register`,
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ messaging_product: 'whatsapp', pin: req.query.pin }),
        }
      );
      const d = await r.json().catch(() => ({}));
      console.log('[wa-bot] register', r.status, JSON.stringify(d).slice(0, 300));
      res.json({ status: r.status, resposta: d });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Perfil comercial do número: foto (assets/whatsapp-profile.png via Resumable
  // Upload API) + descrição/site/categoria. GET /api/whatsapp/profile?token=...
  app.get('/api/whatsapp/profile', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    try {
      const fs = require('fs');
      const path = require('path');
      const token = process.env.WHATSAPP_TOKEN;
      const appId = '2907807466219506';
      const img = fs.readFileSync(path.join(__dirname, 'assets', 'whatsapp-profile.png'));

      // 1) sessão de upload
      const sess = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${appId}/uploads?file_length=${img.length}&file_type=image/png`,
        { method: 'POST', headers: { Authorization: `Bearer ${token}` } }
      ).then((r) => r.json());
      if (!sess.id) return res.status(500).json({ etapa: 'sessao', resposta: sess });

      // 2) envio dos bytes -> handle
      const up = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/${sess.id}`, {
        method: 'POST',
        headers: { Authorization: `OAuth ${token}`, file_offset: '0' },
        body: img,
      }).then((r) => r.json());
      if (!up.h) return res.status(500).json({ etapa: 'upload', resposta: up });

      // 3) aplica foto + textos do perfil
      const prof = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${process.env.WHATSAPP_PHONE_ID}/whatsapp_business_profile`,
        {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            messaging_product: 'whatsapp',
            profile_picture_handle: up.h,
            about: 'Sua terra com o valor certo. 🌱',
            description:
              'Marketplace de terras e imóveis rurais com análise técnica por satélite. Atendimento AdGain: planos, créditos, anúncios e análises.',
            websites: ['https://www.adgain.com.br'],
            vertical: 'PROF_SERVICES',
          }),
        }
      ).then((r) => r.json());
      console.log('[wa-bot] perfil atualizado:', JSON.stringify(prof).slice(0, 200));
      res.json({ foto: 'ok', perfil: prof });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Diagnóstico: últimos statuses de entrega (protegido pelo verify token).
  // GET /api/whatsapp/status?token=...          -> lista últimos statuses
  // GET /api/whatsapp/status?token=...&ping=1   -> dispara um envio de teste antes
  app.get('/api/whatsapp/status', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    if (req.query.ping) {
      await waSend({
        messaging_product: 'whatsapp',
        to: req.query.ping === '1' ? '556599757621' : String(req.query.ping),
        type: 'text',
        text: { body: '🔧 Teste automático de entrega AdGain — pode ignorar.' },
      });
    }
    res.json({ statuses: lastStatuses });
  });

  console.log('[wa-bot] Rotas do bot WhatsApp registradas (/api/whatsapp/webhook)');
};

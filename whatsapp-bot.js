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

  anunciar:
    '🏡 *Como anunciar sua propriedade na AdGain*\n\n' +
    '1️⃣ Acesse www.adgain.com.br e faça login\n' +
    '2️⃣ Clique em *Anunciar* e escolha o caminho:\n' +
    '▫️ *Pelo mapa*: selecione sua parcela (SIGEF/CAR) e, se quiser, rode a análise técnica — ela vira um selo de qualidade no anúncio\n' +
    '▫️ *Cadastro manual*: preencha os dados direto no formulário\n' +
    '3️⃣ Adicione fotos, valor e publique!\n\n' +
    '💡 Se parar no meio, seu rascunho fica salvo e você continua de onde parou, em qualquer dispositivo.',

  analise:
    '🛰️ *Análise técnica AdGain — o raio-X da sua terra*\n\n' +
    'Você seleciona a parcela no mapa e em ~2 minutos recebe:\n' +
    '▫️ CAR e conformidade ambiental\n' +
    '▫️ Desmatamento (PRODES/DETER) e queimadas\n' +
    '▫️ Solos, relevo, clima e recursos hídricos\n' +
    '▫️ Aptidão para pivôs centrais e fontes de água\n' +
    '▫️ Infraestrutura, logística e laudo geológico por IA\n\n' +
    'Tudo pode virar um *relatório completo* para valorizar seu anúncio ou apoiar sua decisão de compra.\n\n' +
    'Se sua análise apresentou algum problema, digite *humano* que nossa equipe verifica para você.',

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
const PROCESSED_TTL_MS = 10 * 60 * 1000;
const HUMAN_MODE_TTL_MS = 12 * 60 * 60 * 1000;

function _sweep(map, ttl) {
  const now = Date.now();
  for (const [k, t] of map) if (now - t > ttl) map.delete(k);
}
setInterval(() => {
  _sweep(processedMsgs, PROCESSED_TTL_MS);
  _sweep(humanMode, HUMAN_MODE_TTL_MS);
}, 5 * 60 * 1000).unref();

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

function sendMenu(to) {
  return waSend({
    to,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: { text: KB.saudacao },
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
              { id: 'op_humano', title: '4️⃣ Falar com humano', description: 'Atendimento com a nossa equipe' },
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
  op_planos: (to) => sendText(to, KB.planos),
  op_anunciar: (to) => sendText(to, KB.anunciar),
  op_analise: (to) => sendText(to, KB.analise),
  op_humano: async (to, profileName) => {
    humanMode.set(to, Date.now());
    await sendText(to, KB.humano);
    const admin = process.env.ADMIN_WHATSAPP;
    if (admin) {
      await sendText(
        admin,
        `🔔 *Pedido de atendimento humano*\nCliente: ${profileName || 'sem nome'} (wa.me/${to})\nResponda pelo WhatsApp Business.`
      );
    }
  },
};

// Atalhos digitados (1-4, palavras-chave)
function matchShortcut(text) {
  const t = (text || '').trim().toLowerCase();
  if (/^(1|planos?|creditos?|créditos?|preços?|precos?)$/.test(t)) return 'op_planos';
  if (/^(2|anunciar|anuncio|anúncio|vender)$/.test(t)) return 'op_anunciar';
  if (/^(3|analise|análise|relatorio|relatório)$/.test(t)) return 'op_analise';
  if (/^(4|humano|atendente|pessoa|suporte)$/.test(t)) return 'op_humano';
  if (/^(menu|oi|ola|olá|bom dia|boa tarde|boa noite|inicio|início|start)$/.test(t)) return 'menu';
  return null;
}

async function handleIncomingMessage(msg, contacts) {
  const from = msg.from; // wa_id do cliente (E.164 sem '+')
  const profileName = contacts && contacts[0] && contacts[0].profile && contacts[0].profile.name;

  // Cliente em modo humano: bot fica em silêncio (a equipe responde pelo app)
  if (humanMode.has(from)) {
    // "menu" reativa o bot
    const t = msg.type === 'text' ? (msg.text.body || '').trim().toLowerCase() : '';
    if (t === 'menu') {
      humanMode.delete(from);
      return sendMenu(from);
    }
    return;
  }

  // Resposta de menu interativo (list)
  if (msg.type === 'interactive') {
    const replyId =
      (msg.interactive.list_reply && msg.interactive.list_reply.id) ||
      (msg.interactive.button_reply && msg.interactive.button_reply.id);
    const handler = OPTION_HANDLERS[replyId];
    if (handler) return handler(from, profileName);
    return sendMenu(from);
  }

  // Texto digitado
  if (msg.type === 'text') {
    const shortcut = matchShortcut(msg.text.body);
    if (shortcut === 'menu') return sendMenu(from);
    if (shortcut && OPTION_HANDLERS[shortcut]) return OPTION_HANDLERS[shortcut](from, profileName);
    // Fase C: pergunta livre -> Claude com base de conhecimento.
    return sendText(from, KB.foraDoEscopo);
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

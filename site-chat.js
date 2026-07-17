// ============================================================
// Chat do site AdGain — POST /api/chat
//
// Mesmo cérebro do bot do WhatsApp (bot-knowledge.js), com:
// - identidade opcional: header x-firebase-token (ID token do usuário logado)
//   -> contexto de nome/plano nas respostas
// - proteção de custo: limite por IP (20/h anônimo, 60/h logado) e teto
//   global diário; mensagens limitadas em tamanho e quantidade
// ============================================================

const Anthropic = require('@anthropic-ai/sdk');
const { getDb } = require('./firebase');
const { PLAN_NAMES, buildSystemPrompt } = require('./bot-knowledge');

let anthropicClient = null;
function getAnthropic() {
  if (!anthropicClient && process.env.ANTHROPIC_API_KEY) {
    anthropicClient = new Anthropic();
  }
  return anthropicClient;
}

// Limites (em memória — suficiente para 1 instância)
const ipHits = new Map(); // ip -> { count, resetTs }
const IP_WINDOW_MS = 60 * 60 * 1000;
const IP_LIMIT_ANON = 20;
const IP_LIMIT_AUTH = 60;
const GLOBAL_DAY_LIMIT = 1500;
let globalDay = { day: '', count: 0 };

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of ipHits) if (now > v.resetTs) ipHits.delete(k);
}, 10 * 60 * 1000).unref();

function checkLimits(ip, autenticado) {
  const hoje = new Date().toISOString().slice(0, 10);
  if (globalDay.day !== hoje) globalDay = { day: hoje, count: 0 };
  if (globalDay.count >= GLOBAL_DAY_LIMIT) return 'global';
  const lim = autenticado ? IP_LIMIT_AUTH : IP_LIMIT_ANON;
  const hit = ipHits.get(ip);
  if (!hit || Date.now() > hit.resetTs) {
    ipHits.set(ip, { count: 1, resetTs: Date.now() + IP_WINDOW_MS });
  } else {
    if (hit.count >= lim) return 'ip';
    hit.count++;
  }
  globalDay.count++;
  return null;
}

function planoDoDoc(d) {
  if (d.subscriptionPlan && (!d.subscriptionStatus || d.subscriptionStatus === 'active')) {
    return d.subscriptionPlan;
  }
  if (d.profile && d.profile.subscription && d.profile.subscription.planType) {
    return d.profile.subscription.planType;
  }
  return 'gratuito';
}

async function identifyUser(idToken) {
  try {
    if (!idToken) return null;
    const db = getDb();
    if (!db) return null;
    const { getAuth } = require('firebase-admin/auth');
    const decoded = await getAuth().verifyIdToken(idToken);
    const snap = await db.collection('users').doc(decoded.uid).get();
    if (!snap.exists) return { uid: decoded.uid, nome: decoded.name || null, plano: 'gratuito' };
    const d = snap.data() || {};
    return { uid: decoded.uid, nome: d.displayName || decoded.name || null, plano: planoDoDoc(d) };
  } catch (err) {
    console.warn('[site-chat] token inválido:', err.message);
    return null;
  }
}

// Visitante que preencheu o formulário (nome + telefone): tenta reconhecer
// pelo phoneDigits (mesma lógica do bot do WhatsApp) e registra o lead.
function phoneVariants(telefone) {
  let d = String(telefone || '').replace(/\D+/g, '');
  if (d.length === 10 || d.length === 11) d = '55' + d;
  if (d.length < 12) return [];
  const out = new Set([d]);
  if (d.length === 13 && d.startsWith('55') && d[4] === '9') out.add(d.slice(0, 4) + d.slice(5));
  if (d.length === 12 && d.startsWith('55')) out.add(d.slice(0, 4) + '9' + d.slice(4));
  return [...out];
}

async function identifyLead(lead) {
  if (!lead || !lead.telefone) return null;
  const variants = phoneVariants(lead.telefone);
  const nomeForm = String(lead.nome || '').slice(0, 80).trim() || null;
  try {
    const db = getDb();
    if (!db) return nomeForm ? { nome: nomeForm, plano: 'gratuito' } : null;
    // registra/atualiza o lead (fire-and-forget)
    if (variants[0]) {
      db.collection('site_chat_leads')
        .doc(variants[0])
        .set(
          { nome: nomeForm, telefone: variants[0], ultimoContato: new Date().toISOString() },
          { merge: true }
        )
        .catch(() => {});
    }
    for (const v of variants) {
      const snap = await db
        .collection('users')
        .where('phoneDigits', 'array-contains', v)
        .limit(1)
        .get();
      if (!snap.empty) {
        const d = snap.docs[0].data() || {};
        return { uid: snap.docs[0].id, nome: d.displayName || nomeForm, plano: planoDoDoc(d) };
      }
    }
  } catch (err) {
    console.warn('[site-chat] lookup por telefone falhou:', err.message);
  }
  return nomeForm ? { nome: nomeForm, plano: 'gratuito' } : null;
}

module.exports = function registerSiteChat(app) {
  app.post('/api/chat', async (req, res) => {
    try {
      const client = getAnthropic();
      if (!client) return res.status(503).json({ erro: 'chat indisponível' });

      // Validação do corpo
      const mensagens = Array.isArray(req.body && req.body.mensagens) ? req.body.mensagens : null;
      if (!mensagens || !mensagens.length || mensagens.length > 12) {
        return res.status(400).json({ erro: 'mensagens inválidas' });
      }
      const limpas = mensagens
        .filter(
          (m) =>
            m &&
            (m.role === 'user' || m.role === 'assistant') &&
            typeof m.content === 'string' &&
            m.content.trim()
        )
        .map((m) => ({ role: m.role, content: m.content.slice(0, 600) }));
      if (!limpas.length || limpas[limpas.length - 1].role !== 'user') {
        return res.status(400).json({ erro: 'mensagens inválidas' });
      }

      // Identidade (opcional) e limites: login > telefone do formulário > anônimo
      const user =
        (await identifyUser(req.headers['x-firebase-token'])) ||
        (await identifyLead(req.body && req.body.lead));
      const ip =
        (req.headers['x-forwarded-for'] || '').split(',')[0].trim() ||
        req.socket.remoteAddress ||
        'desconhecido';
      const bloqueio = checkLimits(ip, !!user);
      if (bloqueio) {
        return res.status(429).json({
          resposta:
            'Estou recebendo muitas mensagens agora. 🙏 Tente novamente em alguns minutos, ou fale com a equipe no WhatsApp: wa.me/556596679565',
        });
      }

      const resp = await client.messages.create({
        model: 'claude-haiku-4-5',
        max_tokens: 400,
        system: await buildSystemPrompt('site', user),
        messages: limpas.slice(-8),
      });
      const resposta = resp.content
        .filter((b) => b.type === 'text')
        .map((b) => b.text)
        .join('')
        .trim();
      if (!resposta) return res.status(502).json({ erro: 'sem resposta' });
      res.json({ resposta, plano: user ? PLAN_NAMES[user.plano] || user.plano : null });
    } catch (err) {
      console.error('[site-chat] erro:', err.message);
      res.status(500).json({ erro: 'falha no chat' });
    }
  });

  console.log('[site-chat] Rota registrada (/api/chat)');
};

// ============================================================
// Cérebro do assistente AdGain — compartilhado entre os canais
// (bot do WhatsApp e chat do site). Conhecimento + preços vivos.
// ============================================================

const { getDb } = require('./firebase');

const PLAN_NAMES = {
  gratuito: 'Gratuito',
  essencial: 'Essencial',
  profissional: 'Profissional',
  empresarial: 'Empresarial',
  premium: 'Premium',
  enterprise: 'Premium',
};
const PRIORITY_PLANS = ['empresarial', 'premium', 'enterprise'];

// Fallback estático caso o Firestore esteja indisponível
const PLANOS_FALLBACK =
  '🌱 *Planos AdGain* (por mês):\n\n' +
  '▫️ *Gratuito* — R$ 0 (3 fotos por anúncio)\n' +
  '▫️ *Essencial* — R$ 12,00 (30 créditos/mês, 5 fotos)\n' +
  '▫️ *Profissional* — R$ 19,90 (100 créditos/mês, 10 fotos, estatísticas)\n' +
  '▫️ *Empresarial* — R$ 49,90 (400 créditos/mês, 15 fotos, analytics completo)\n' +
  '▫️ *Premium* — R$ 99,90 (800 créditos/mês, 15 fotos, analytics completo, suporte VIP)\n\n' +
  'Compare e assine em: www.adgain.com.br/plans';

// Preços vivos (credit_config/pricing — mesma fonte do site/checkout)
let pricingCache = { text: null, ts: 0 };
const PRICING_CACHE_TTL_MS = 10 * 60 * 1000;

async function getPlanosText() {
  if (pricingCache.text && Date.now() - pricingCache.ts < PRICING_CACHE_TTL_MS) {
    return pricingCache.text;
  }
  try {
    const db = getDb();
    if (db) {
      const doc = await db.doc('credit_config/pricing').get();
      const plans = doc.exists && Array.isArray(doc.data().plans) ? doc.data().plans : null;
      if (plans && plans.length) {
        const fmt = (c) =>
          !c || c <= 0 ? 'Grátis' : 'R$ ' + (c / 100).toFixed(2).replace('.', ',') + '/mês';
        const linhas = plans
          .slice()
          .sort((a, b) => (a.displayOrder || 0) - (b.displayOrder || 0))
          .map((p) => {
            const extras = [];
            if (p.creditsPerMonth) extras.push(`${p.creditsPerMonth} créditos/mês`);
            if (p.maxPhotosPerAd) extras.push(`${p.maxPhotosPerAd} fotos por anúncio`);
            return `▫️ *${p.name}* — ${fmt(p.priceInCents)}${extras.length ? `\n   ${extras.join(' • ')}` : ''}`;
          });
        pricingCache = {
          text:
            '🌱 *Planos AdGain*:\n\n' +
            linhas.join('\n') +
            '\n\nCompare os recursos e assine em: www.adgain.com.br/plans',
          ts: Date.now(),
        };
        return pricingCache.text;
      }
    }
  } catch (err) {
    console.error('[bot] preços vivos falharam:', err.message);
  }
  return PLANOS_FALLBACK;
}

// Conhecimento central (neutro de canal)
const BOT_CORE = `Você é o assistente virtual oficial da AdGain (www.adgain.com.br), marketplace brasileiro de compra e venda de terras e imóveis rurais com análise técnica por satélite.

SOBRE A ADGAIN:
- Anunciantes publicam propriedades rurais (fazendas, sítios, chácaras, lotes) e compradores as encontram no site.
- Diferencial: a ANÁLISE TÉCNICA — o usuário seleciona a parcela no mapa (SIGEF/CAR) e em ~2 minutos recebe um raio-X da terra: CAR e conformidade ambiental, desmatamento (PRODES/DETER), queimadas, solos, relevo, clima, recursos hídricos, aptidão para pivôs centrais, infraestrutura, logística e laudo geológico por IA. A análise pode virar relatório completo e selo de qualidade no anúncio.
- CRÉDITOS: moeda interna do site. Servem para desbloquear seções de análises e relatórios. Assinantes ganham créditos todo mês (conforme o plano) e qualquer um pode comprar créditos avulsos. Anunciantes ganham parte dos créditos (reward) quando compradores desbloqueiam seções do anúncio deles.
- COMO ANUNCIAR: entrar em www.adgain.com.br → Anunciar → escolher "pelo mapa" (seleciona a parcela SIGEF/CAR e pode rodar a análise) ou "cadastro manual". O rascunho fica salvo e sincroniza entre dispositivos. Quantidade de fotos por anúncio depende do plano.
- ESTATÍSTICAS: planos pagos têm painel de estatísticas básicas dos anúncios; Empresarial e Premium têm analytics completo por anúncio (gráficos, funil, origem do tráfego, PDF).
- SUPORTE HUMANO: seg-sex, 8h às 18h.

REGRAS DE RESPOSTA:
- Responda em português brasileiro, tom cordial e direto, mensagens CURTAS (idealmente até 500 caracteres), sem cabeçalhos, use *negrito* com moderação e no máximo 1-2 emojis.
- Use APENAS as informações deste prompt (incluindo os preços abaixo). NUNCA invente preços, prazos, funcionalidades ou políticas. Se não souber ou o assunto for delicado (pagamento não reconhecido, problema técnico, cancelamento, dados pessoais), diga que a equipe humana pode ajudar e oriente conforme o canal.
- Só trate de assuntos da AdGain. Para qualquer outro tema, redirecione com simpatia para o que você pode ajudar.
- Não peça nem registre dados sensíveis (senhas, cartões).`;

const CHANNEL_RULES = {
  whatsapp:
    '\n\nCANAL: você atende pelo WhatsApp oficial da AdGain. Digitar *menu* mostra o menu de opções.',
  site:
    '\n\nCANAL: você atende pelo chat do site www.adgain.com.br. O visitante já está no site, então em vez de dizer "acesse o site", indique o caminho direto (ex.: "clique em Anunciar no menu").',
};

// Escadinha de suporte: menu -> Claude -> humano SÓ para plano pago ativo
// (fila prioritária para Empresarial/Premium)
function supportPolicy(canal, user) {
  const pago = user && user.plano && user.plano !== 'gratuito';
  if (pago) {
    const prior = PRIORITY_PLANS.includes(user.plano);
    const como =
      canal === 'whatsapp'
        ? 'oriente o cliente a digitar *humano*'
        : 'informe o WhatsApp da equipe: wa.me/556596679565 (65 99667-9565)';
    return (
      `\n\nPOLÍTICA DE SUPORTE PARA ESTE CLIENTE: assinante ${PLAN_NAMES[user.plano] || user.plano}` +
      `${prior ? ' com FILA PRIORITÁRIA' : ''} — tem direito a atendimento humano (seg-sex, 8h-18h). ` +
      `SEMPRE tente resolver você primeiro; apenas se não conseguir, ou em caso delicado ` +
      `(pagamento, problema técnico, cancelamento, dados pessoais), ${como}.`
    );
  }
  return (
    '\n\nPOLÍTICA DE SUPORTE PARA ESTE USUÁRIO: o atendimento humano é benefício exclusivo ' +
    'dos planos pagos. NÃO ofereça atendimento humano nem repasse contatos da equipe — ' +
    'esforce-se ao máximo para resolver aqui mesmo. Se pedirem um atendente, explique com ' +
    'simpatia que o suporte com a equipe faz parte dos planos pagos e convide a conhecer: ' +
    'www.adgain.com.br/plans.'
  );
}

async function buildSystemPrompt(canal, user) {
  const precos = await getPlanosText();
  const contexto = user
    ? `\n\nCLIENTE ATUAL: ${user.nome || 'sem nome'} — plano ${PLAN_NAMES[user.plano] || user.plano}. Personalize quando fizer sentido.`
    : '\n\nCLIENTE ATUAL: visitante não identificado (não logado/cadastrado).';
  return (
    BOT_CORE +
    (CHANNEL_RULES[canal] || '') +
    `\n\nPREÇOS E PLANOS ATUAIS (fonte oficial, use exatamente estes valores):\n${precos}` +
    contexto +
    supportPolicy(canal, user)
  );
}

// Respostas prontas (custo zero — não acionam o Claude), compartilhadas
// entre o menu do WhatsApp e os atalhos do chat do site
const CANNED = {
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
    'Tudo pode virar um *relatório completo* para valorizar seu anúncio ou apoiar sua decisão de compra.',

  ganhos:
    '📈 *Ganhe ao anunciar na AdGain*\n\n' +
    'Seu anúncio pode gerar renda antes mesmo da venda:\n\n' +
    '▫️ Compradores usam créditos para desbloquear seções do seu anúncio (análises, relatórios, contato)\n' +
    '▫️ Parte desses créditos vira *recompensa sua*, automaticamente\n' +
    '▫️ Quanto mais completo o anúncio (fotos + análise técnica), mais desbloqueios — e mais ganhos\n' +
    '▫️ Acompanhe tudo na aba *Meus Ganhos* da página de planos e solicite *saque via Pix* quando quiser\n\n' +
    'Comece em: www.adgain.com.br/plans',

  creditos:
    '💳 *Créditos AdGain — como funcionam*\n\n' +
    'Créditos são a moeda interna do site. Com eles você desbloqueia seções de análises e relatórios.\n\n' +
    '▫️ *Assinantes* ganham créditos todo mês, conforme o plano\n' +
    '▫️ Qualquer pessoa pode *comprar créditos avulsos* na página de planos\n' +
    '▫️ *Anunciantes ganham de volta*: quando um comprador desbloqueia seções do seu anúncio, parte dos créditos vira recompensa para você\n\n' +
    'Veja os pacotes em: www.adgain.com.br/plans',
};

async function cannedAnswer(id) {
  if (id === 'planos') return getPlanosText();
  return CANNED[id] || null;
}

module.exports = {
  PLAN_NAMES,
  PRIORITY_PLANS,
  PLANOS_FALLBACK,
  getPlanosText,
  buildSystemPrompt,
  CANNED,
  cannedAnswer,
};

// ============================================================
// Campanha via WhatsApp Cloud API (número do robô 65 9667-9565).
//
// Caminho pago e oficial para apresentar a AdGain a listas frias:
// template de MARKETING aprovado pela Meta, enviado pelo número do
// robô — quem responde já cai no atendimento automático do bot.
// O 8127 (linha empresarial) fica fora disso por completo.
//
// Endpoints (todos protegidos pelo WHATSAPP_VERIFY_TOKEN):
//   GET  /api/whatsapp/campanha/template?token=...        -> lista templates e status
//   GET  /api/whatsapp/campanha/template?token=...&do=1   -> cria o template p/ aprovação
//   POST /api/whatsapp/campanha/enviar?token=...          -> dispara o template
//        body: { numeros: ["5566...", ...], limite: 50, teste: true|false }
//   GET  /api/whatsapp/campanha/status?token=...          -> placar (enviados/erros)
//
// Controle de duplicados: coleção `wa_campanha` no Firestore
// (1 doc por telefone) — reenvio para o mesmo número é ignorado.
// ============================================================

const { getDb } = require('./firebase');

const GRAPH_VERSION = 'v23.0';
const TEMPLATE_NAME = 'apresentacao_adgain_corretores';
const TEMPLATE_IMG = 'apresentacao_adgain_curto';
const TEMPLATE_LANG = 'pt_BR';

// Texto do template (sem variáveis => aprovação mais simples).
// Curto de propósito: com imagem no cabeçalho o WhatsApp esconde textos
// longos atrás do "Ler mais" — este cabe inteiro na primeira dobra.
// O opt-out fica por conta do botão "Não tenho interesse".
const TEMPLATE_BODY =
  'Olá! Aqui é o Nilton, da AdGain — plataforma de compra e venda de terras rurais. ' +
  'Vi seu contato em um grupo de fazendas.\n\n' +
  'Anunciar na AdGain é de graça: sem mensalidade, sem comissão e sem exclusividade. ' +
  'E você ganha créditos quando um interessado desbloqueia seu anúncio.\n\n' +
  'Quer saber mais? É só responder — nosso assistente atende na hora.';

function auth(req, res) {
  if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
    res.sendStatus(403);
    return false;
  }
  return true;
}

function headers() {
  return {
    Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
    'Content-Type': 'application/json',
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

module.exports = function registerCampanha(app) {
  // ---------- template ----------
  app.get('/api/whatsapp/campanha/template', async (req, res) => {
    if (!auth(req, res)) return;
    const waba = process.env.WHATSAPP_WABA_ID || '1011685214925033';
    try {
      if (req.query.do) {
        const r = await fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates`,
          {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({
              name: TEMPLATE_NAME,
              language: TEMPLATE_LANG,
              category: 'MARKETING',
              components: [
                { type: 'BODY', text: TEMPLATE_BODY },
                {
                  type: 'BUTTONS',
                  buttons: [
                    { type: 'QUICK_REPLY', text: 'Quero conhecer' },
                    { type: 'QUICK_REPLY', text: 'Não tenho interesse' },
                    {
                      type: 'URL',
                      text: 'Cadastro grátis',
                      url: 'https://www.adgain.com.br/auth/register',
                    },
                  ],
                },
              ],
            }),
          }
        );
        const d = await r.json().catch(() => ({}));
        console.log('[campanha] criar template', r.status, JSON.stringify(d).slice(0, 300));
        return res.status(r.ok ? 200 : 502).json({ criado: r.ok, resposta: d });
      }
      const r = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates?fields=name,status,category,quality_score,rejected_reason&limit=50`,
        { headers: headers() }
      );
      const d = await r.json().catch(() => ({}));
      res.status(r.ok ? 200 : 502).json(d);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ---------- versão com criativo no cabeçalho ----------
  // POST body: { imagemBase64, mime } — sobe a imagem (resumable upload p/ o
  // exemplo do template + /media p/ os envios), cria o template *_img e grava
  // a configuração em wa_campanha_config/global.
  app.post('/api/whatsapp/campanha/header', async (req, res) => {
    if (!auth(req, res)) return;
    const db = getDb();
    if (!db) return res.status(500).json({ error: 'Firestore indisponível' });
    const { imagemBase64, mime, nome, corpo, semImagem } = req.body || {};
    if (!imagemBase64 && !semImagem) return res.status(400).json({ error: 'imagemBase64 obrigatório (ou semImagem: true)' });
    const nomeTemplate = String(nome || TEMPLATE_IMG).replace(/[^a-z0-9_]/g, '');
    const corpoTemplate = String(corpo || TEMPLATE_BODY);
    const buf = Buffer.from(imagemBase64, 'base64');
    const tipo = mime || 'image/jpeg';
    const token = process.env.WHATSAPP_TOKEN;
    const waba = process.env.WHATSAPP_WABA_ID || '1011685214925033';
    try {
      if (semImagem) {
        console.log('[campanha/header] semImagem: criando', nomeTemplate, corpoTemplate.length, 'chars');
        const tplTxt = await fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates`,
          {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({
              name: nomeTemplate,
              language: TEMPLATE_LANG,
              category: 'MARKETING',
              components: [
                { type: 'BODY', text: corpoTemplate },
                {
                  type: 'BUTTONS',
                  buttons: [
                    { type: 'QUICK_REPLY', text: 'Quero conhecer' },
                    { type: 'QUICK_REPLY', text: 'Não tenho interesse' },
                    { type: 'URL', text: 'Cadastro grátis', url: 'https://www.adgain.com.br/auth/register' },
                  ],
                },
              ],
            }),
          }
        ).then((r) => r.json());
        await db.collection('wa_campanha_config').doc('global').set(
          { template: nomeTemplate, headerMediaId: null, atualizadoEm: new Date() },
          { merge: true }
        );
        console.log('[campanha/header] semImagem resposta Meta:', JSON.stringify(tplTxt).slice(0, 200));
        return res.json({ ok: true, semImagem: true, template: tplTxt });
      }
      // 1) app dono do token (necessário para o resumable upload)
      const appInfo = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/app?access_token=${encodeURIComponent(token)}`
      ).then((r) => r.json());
      if (!appInfo.id) return res.status(502).json({ etapa: 'app', resposta: appInfo });

      // 2) resumable upload -> header_handle (exemplo exigido pela análise)
      const sessao = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${appInfo.id}/uploads?file_length=${buf.length}&file_type=${encodeURIComponent(tipo)}&access_token=${encodeURIComponent(token)}`,
        { method: 'POST' }
      ).then((r) => r.json());
      if (!sessao.id) return res.status(502).json({ etapa: 'upload-sessao', resposta: sessao });
      const upload = await fetch(`https://graph.facebook.com/${GRAPH_VERSION}/${sessao.id}`, {
        method: 'POST',
        headers: { Authorization: `OAuth ${token}`, file_offset: '0' },
        body: buf,
      }).then((r) => r.json());
      if (!upload.h) return res.status(502).json({ etapa: 'upload', resposta: upload });

      // 3) /media do número -> id reutilizado em cada envio
      const fd = new FormData();
      fd.append('messaging_product', 'whatsapp');
      fd.append('file', new Blob([buf], { type: tipo }), 'criativo.jpg');
      const media = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${process.env.WHATSAPP_PHONE_ID}/media`,
        { method: 'POST', headers: { Authorization: `Bearer ${token}` }, body: fd }
      ).then((r) => r.json());
      if (!media.id) return res.status(502).json({ etapa: 'media', resposta: media });

      // 4) template com imagem no cabeçalho
      const tpl = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates`,
        {
          method: 'POST',
          headers: headers(),
          body: JSON.stringify({
            name: nomeTemplate,
            language: TEMPLATE_LANG,
            category: 'MARKETING',
            components: [
              { type: 'HEADER', format: 'IMAGE', example: { header_handle: [upload.h] } },
              { type: 'BODY', text: corpoTemplate },
              {
                type: 'BUTTONS',
                buttons: [
                  { type: 'QUICK_REPLY', text: 'Quero conhecer' },
                  { type: 'QUICK_REPLY', text: 'Não tenho interesse' },
                  {
                    type: 'URL',
                    text: 'Cadastro grátis',
                    url: 'https://www.adgain.com.br/auth/register',
                  },
                ],
              },
            ],
          }),
        }
      ).then((r) => r.json());

      await db.collection('wa_campanha_config').doc('global').set(
        { template: nomeTemplate, headerMediaId: media.id, atualizadoEm: new Date() },
        { merge: true }
      );
      console.log('[campanha] template com imagem criado:', JSON.stringify(tpl).slice(0, 200));
      res.json({ ok: true, mediaId: media.id, template: tpl });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ---------- troca só a imagem do cabeçalho (mantém o template já aprovado) ----------
  // POST /api/whatsapp/campanha/header-media?token=...  body: { imagemBase64, mime? }
  // Sobe a imagem nova ao /media do número e atualiza wa_campanha_config/global.headerMediaId
  // SEM criar/reaprovar template — o template aprovado só declara "tem cabeçalho IMAGE";
  // a imagem de fato usada em cada envio é sempre a do headerMediaId atual.
  app.post('/api/whatsapp/campanha/header-media', async (req, res) => {
    if (!auth(req, res)) return;
    const db = getDb();
    if (!db) return res.status(500).json({ error: 'Firestore indisponível' });
    const { imagemBase64, mime } = req.body || {};
    if (!imagemBase64) return res.status(400).json({ error: 'imagemBase64 obrigatório' });
    const buf = Buffer.from(imagemBase64, 'base64');
    const tipo = mime || 'image/jpeg';
    const token = process.env.WHATSAPP_TOKEN;
    try {
      const fd = new FormData();
      fd.append('messaging_product', 'whatsapp');
      fd.append('file', new Blob([buf], { type: tipo }), 'criativo.jpg');
      const media = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${process.env.WHATSAPP_PHONE_ID}/media`,
        { method: 'POST', headers: { Authorization: `Bearer ${token}` }, body: fd }
      ).then((r) => r.json());
      if (!media.id) return res.status(502).json({ etapa: 'media', resposta: media });

      await db.collection('wa_campanha_config').doc('global').set(
        { headerMediaId: media.id, atualizadoEm: new Date() },
        { merge: true }
      );
      console.log('[campanha/header-media] novo headerMediaId:', media.id);
      res.json({ ok: true, mediaId: media.id });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ---------- template de retomada (reabre conversa fora da janela de 24h) ----------
  // GET  ?do=1 cria o template; sem do, lista o status dele.
  // POST /api/whatsapp/campanha/retomada  body {numeros:[...]}  -> envia (pula opt-out)
  app.get('/api/whatsapp/campanha/retomada-template', async (req, res) => {
    if (!auth(req, res)) return;
    const waba = process.env.WHATSAPP_WABA_ID || '1011685214925033';
    try {
      if (req.query.do) {
        const r = await fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates`,
          {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({
              name: 'retomada_adgain',
              language: TEMPLATE_LANG,
              category: 'MARKETING',
              components: [
                {
                  type: 'BODY',
                  text:
                    'Boa tarde! 🌱 Aqui é da AdGain. Passando para saber em que podemos te ajudar — ' +
                    'anunciar sua propriedade, entender a análise técnica ou os créditos que o anúncio gera.\n\n' +
                    'É só responder esta mensagem que a gente continua daqui.',
                },
                {
                  type: 'BUTTONS',
                  buttons: [
                    { type: 'QUICK_REPLY', text: 'Quero anunciar' },
                    { type: 'QUICK_REPLY', text: 'Tenho uma dúvida' },
                    { type: 'QUICK_REPLY', text: 'Não tenho interesse' },
                  ],
                },
              ],
            }),
          }
        );
        const d = await r.json().catch(() => ({}));
        return res.status(r.ok ? 200 : 502).json({ criado: r.ok, resposta: d });
      }
      const r = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates?fields=name,status&limit=50`,
        { headers: headers() }
      );
      const d = await r.json().catch(() => ({}));
      const t = (d.data || []).find((x) => x.name === 'retomada_adgain');
      res.json(t || { status: 'INEXISTENTE' });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/whatsapp/campanha/retomada', async (req, res) => {
    if (!auth(req, res)) return;
    const db = getDb();
    if (!db) return res.status(500).json({ error: 'Firestore indisponível' });
    const numeros = Array.isArray(req.body && req.body.numeros) ? req.body.numeros : [];
    if (!numeros.length) return res.status(400).json({ error: 'numeros obrigatório' });
    const resultados = { enviados: [], pulados: [], erros: [] };
    for (const bruto of numeros) {
      const tel = String(bruto).replace(/\D/g, '');
      if (!tel) continue;
      const optout = await db.collection('wa_optout').doc(tel).get();
      if (optout.exists) { resultados.pulados.push(tel); continue; }
      try {
        const r = await fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${process.env.WHATSAPP_PHONE_ID}/messages`,
          {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({
              messaging_product: 'whatsapp',
              to: tel,
              type: 'template',
              template: { name: 'retomada_adgain', language: { code: TEMPLATE_LANG } },
            }),
          }
        );
        const d = await r.json().catch(() => ({}));
        if (d.messages && d.messages[0]) resultados.enviados.push(tel);
        else resultados.erros.push({ tel, resposta: JSON.stringify(d).slice(0, 150) });
      } catch (err) {
        resultados.erros.push({ tel, resposta: err.message });
      }
      await sleep(1100);
    }
    res.json(resultados);
  });

  // ---------- template só-texto da campanha Chãozão (fixo no código) ----------
  // GET ?do=1 cria; sem do, mostra o status. Mesmo padrão do retomada-template
  // (o POST /header com corpo custom travava neste ambiente).
  app.get('/api/whatsapp/campanha/texto-template', async (req, res) => {
    if (!auth(req, res)) return;
    const waba = process.env.WHATSAPP_WABA_ID || '1011685214925033';
    const CORPO =
      'Olá! Aqui é da AdGain 🌱\n\n' +
      '📢 Anunciar terras é 100% GRÁTIS\n' +
      '💰 E você GANHA: créditos sacáveis via Pix enquanto a propriedade não vende\n' +
      '🎯 Na vitrine, veja quem está COMPRANDO na sua região\n\n' +
      'Responda que a gente te mostra 👇';
    try {
      if (req.query.do) {
        const r = await fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates`,
          {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({
              name: 'gratis_ganha_zap',
              language: TEMPLATE_LANG,
              category: 'MARKETING',
              components: [
                { type: 'BODY', text: CORPO },
                {
                  type: 'BUTTONS',
                  buttons: [
                    { type: 'QUICK_REPLY', text: 'Quero conhecer' },
                    { type: 'QUICK_REPLY', text: 'Não tenho interesse' },
                    { type: 'URL', text: 'Cadastro grátis', url: 'https://www.adgain.com.br/auth/register' },
                  ],
                },
              ],
            }),
          }
        );
        const d = await r.json().catch(() => ({}));
        return res.status(r.ok ? 200 : 502).json({ criado: r.ok, resposta: d });
      }
      const r = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${waba}/message_templates?fields=name,status&limit=50`,
        { headers: headers() }
      );
      const d = await r.json().catch(() => ({}));
      const t = (d.data || []).find((x) => x.name === 'gratis_ganha_zap');
      res.json(t || { status: 'INEXISTENTE' });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ---------- disparo ----------
  app.post('/api/whatsapp/campanha/enviar', async (req, res) => {
    if (!auth(req, res)) return;
    const db = getDb();
    if (!db) return res.status(500).json({ error: 'Firestore indisponível' });

    const body = req.body || {};
    const numeros = Array.isArray(body.numeros) ? body.numeros : [];
    const limite = Math.min(Math.max(parseInt(body.limite, 10) || 50, 1), 250);
    const teste = !!body.teste;
    if (!numeros.length) return res.status(400).json({ error: 'numeros[] obrigatório' });

    const col = db.collection('wa_campanha');
    const resultados = { enviados: [], pulados: [], erros: [] };

    // configuração: template com imagem (se criado/aprovado) ou o texto puro
    const cfgSnap = await db.collection('wa_campanha_config').doc('global').get();
    const cfg = cfgSnap.exists ? cfgSnap.data() : {};
    const nomeTemplate = body.template || cfg.template || TEMPLATE_NAME;
    const headerMediaId = (nomeTemplate === cfg.template || nomeTemplate === TEMPLATE_IMG) ? (cfg.headerMediaId || null) : null;
    resultados.template = nomeTemplate;

    for (const bruto of numeros) {
      if (resultados.enviados.length >= limite) break;
      const tel = String(bruto).replace(/\D+/g, '');
      if (!tel || tel.length < 10) {
        resultados.erros.push({ tel: bruto, erro: 'número inválido' });
        continue;
      }

      // já recebeu ou pediu para sair? pula (nunca reenvia)
      const ref = col.doc(tel);
      const [snap, optout] = await Promise.all([
        ref.get(),
        db.collection('wa_optout').doc(tel).get(),
      ]);
      if (optout.exists || (snap.exists && snap.data().status === 'enviado')) {
        resultados.pulados.push(tel);
        continue;
      }

      if (teste) {
        resultados.enviados.push({ tel, teste: true });
        continue;
      }

      try {
        const r = await fetch(
          `https://graph.facebook.com/${GRAPH_VERSION}/${process.env.WHATSAPP_PHONE_ID}/messages`,
          {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({
              messaging_product: 'whatsapp',
              to: tel,
              type: 'template',
              template: {
                name: nomeTemplate,
                language: { code: TEMPLATE_LANG },
                ...(headerMediaId
                  ? {
                      components: [
                        {
                          type: 'header',
                          parameters: [{ type: 'image', image: { id: headerMediaId } }],
                        },
                      ],
                    }
                  : {}),
              },
            }),
          }
        );
        const d = await r.json().catch(() => ({}));
        if (r.ok && d.messages && d.messages[0]) {
          await ref.set({
            status: 'enviado',
            template: nomeTemplate,
            messageId: d.messages[0].id,
            em: new Date(),
          });
          resultados.enviados.push({ tel, id: d.messages[0].id });
        } else {
          const erro = (d.error && (d.error.message + (d.error.error_data ? ' | ' + JSON.stringify(d.error.error_data) : ''))) || ('HTTP ' + r.status);
          await ref.set({ status: 'erro', template: nomeTemplate, erro, em: new Date() }, { merge: true });
          resultados.erros.push({ tel, erro });
          // erro de pagamento/limite derruba o lote inteiro — para na hora
          if (d.error && [131042, 131048, 131056, 80007].includes(d.error.code)) {
            resultados.abortado = 'erro de cobrança/limite: ' + erro;
            break;
          }
        }
      } catch (err) {
        resultados.erros.push({ tel, erro: err.message });
      }
      await sleep(1100); // ~1 msg/seg — bem abaixo do teto da Meta
    }

    console.log(
      '[campanha] lote: %d enviados, %d pulados, %d erros%s',
      resultados.enviados.length, resultados.pulados.length, resultados.erros.length,
      resultados.abortado ? ' (ABORTADO: ' + resultados.abortado + ')' : ''
    );
    res.json(resultados);
  });

  // ---------- placar ----------
  app.get('/api/whatsapp/campanha/status', async (req, res) => {
    if (!auth(req, res)) return;
    const db = getDb();
    if (!db) return res.status(500).json({ error: 'Firestore indisponível' });
    try {
      const snap = await db.collection('wa_campanha').get();
      let enviados = 0, erros = 0;
      const listaErros = [];
      snap.forEach((d) => {
        const x = d.data();
        if (x.status === 'enviado') enviados++;
        else { erros++; listaErros.push({ tel: d.id, erro: x.erro }); }
      });
      res.json({ enviados, erros, listaErros: listaErros.slice(0, 20) });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
};

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
const TEMPLATE_LANG = 'pt_BR';

// Texto do template (sem variáveis => aprovação mais simples).
// Regras seguidas: identifica a empresa, diz de onde veio o contato,
// oferece saída clara (opt-out) e não usa CAIXA ALTA exagerada.
const TEMPLATE_BODY =
  'Olá! Aqui é o Nilton, da AdGain — plataforma de compra e venda de terras rurais. ' +
  'Vi seu contato em um grupo de negócios de fazendas no WhatsApp.\n\n' +
  'Na AdGain, anunciar fazenda é de graça: sem mensalidade, sem comissão e sem ' +
  'exclusividade. E você ainda ganha créditos quando um interessado desbloqueia ' +
  'informações do seu anúncio.\n\n' +
  'Quer saber como funciona? É só responder esta mensagem — nosso assistente ' +
  'responde na hora. Se preferir não receber mais novidades, toque em "Não tenho interesse".';

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

    for (const bruto of numeros) {
      if (resultados.enviados.length >= limite) break;
      const tel = String(bruto).replace(/\D+/g, '');
      if (!tel || tel.length < 10) {
        resultados.erros.push({ tel: bruto, erro: 'número inválido' });
        continue;
      }

      // já recebeu? pula (nunca reenvia)
      const ref = col.doc(tel);
      const snap = await ref.get();
      if (snap.exists && snap.data().status === 'enviado') {
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
              template: { name: TEMPLATE_NAME, language: { code: TEMPLATE_LANG } },
            }),
          }
        );
        const d = await r.json().catch(() => ({}));
        if (r.ok && d.messages && d.messages[0]) {
          await ref.set({
            status: 'enviado',
            template: TEMPLATE_NAME,
            messageId: d.messages[0].id,
            em: new Date(),
          });
          resultados.enviados.push({ tel, id: d.messages[0].id });
        } else {
          const erro = (d.error && (d.error.message + (d.error.error_data ? ' | ' + JSON.stringify(d.error.error_data) : ''))) || ('HTTP ' + r.status);
          await ref.set({ status: 'erro', template: TEMPLATE_NAME, erro, em: new Date() }, { merge: true });
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

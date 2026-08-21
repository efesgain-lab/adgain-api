// ============================================================
// Diagnóstico de cadastros — GET /api/diag/cadastros?token=...
//
// Responde a pergunta de produto: "pedir telefone no cadastro vale a pena?".
// Mede quantos usuários HOJE ficam sem telefone e se eles voltam ao app.
//
// PRIVACIDADE: devolve apenas CONTAGENS agregadas. Nenhum telefone, e-mail,
// nome ou CPF sai daqui.
// ============================================================

const { getDb } = require('./firebase');

/** Telefone do cadastro do usuário, em qualquer um dos formatos já usados. */
function temTelefoneNoPerfil(u) {
  const p = u.profile || {};
  const candidatos = [
    p.phones && p.phones.primary && p.phones.primary.number,
    p.phones && p.phones.secondary && p.phones.secondary.number,
    p.phone,
    p.whatsapp,
    u.phone,
    u.phoneNumber,
    u.whatsapp,
    Array.isArray(u.phoneDigits) && u.phoneDigits.length ? u.phoneDigits[0] : null,
  ];
  return candidatos.some((v) => !!String(v || '').replace(/\D/g, ''));
}

/** Telefone informado dentro do anúncio (ownerContact/contact). */
function telefoneDoAnuncio(prop) {
  const oc = prop.ownerContact || {};
  const c = prop.contact || {};
  const candidatos = [oc.whatsapp, oc.phone, c.whatsapp, c.contactPhone, c.phone, prop.phone];
  return candidatos.some((v) => !!String(v || '').replace(/\D/g, ''));
}

module.exports = (app) => {
  app.get('/api/diag/cadastros', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    const db = getDb();
    if (!db) return res.status(503).json({ erro: 'Firestore indisponivel' });

    try {
      const [usersSnap, propsSnap, intentsSnap] = await Promise.all([
        db.collection('users').get(),
        db.collection('properties').get(),
        db.collection('intents-to-buy').get(),
      ]);

      // Intencoes de compra: base da futura "busca por compradores"
      const intents = { total: 0, ativas: 0, querNotificacao: 0, comFiltroReal: 0 };
      intentsSnap.forEach((doc) => {
        const i = doc.data() || {};
        intents.total++;
        if (i.isActive !== false) intents.ativas++;
        if (i.receiveNotifications) intents.querNotificacao++;
        const temUF = i.location && Array.isArray(i.location.states) && i.location.states.length;
        const temTipo = Array.isArray(i.propertyTypes) && i.propertyTypes.length;
        const temArea = i.areaRange && (i.areaRange.min > 0 || i.areaRange.max > 0);
        const temPreco = i.priceRange && (i.priceRange.min > 0 || i.priceRange.max > 0);
        if (temUF || temTipo || temArea || temPreco) intents.comFiltroReal++;
      });

      // Donos que informaram telefone no anúncio (fonte paralela ao perfil)
      const donosComTelefoneNoAnuncio = new Set();
      const donosComAnuncio = new Set();
      propsSnap.forEach((doc) => {
        const p = doc.data() || {};
        const dono = (p.listing && p.listing.ownerId) || p.ownerId;
        if (!dono) return;
        donosComAnuncio.add(dono);
        if (telefoneDoAnuncio(p)) donosComTelefoneNoAnuncio.add(dono);
      });

      const r = {
        intencoesDeCompra: intents,
        totalUsuarios: 0,
        comTelefoneNoPerfil: 0,
        semTelefoneNoPerfil: 0,
        // dos sem telefone no perfil, quantos têm o número "escondido" no anúncio
        semNoPerfilMasTemNoAnuncio: 0,
        semTelefoneEmLugarNenhum: 0,
        // comportamento: quem nunca voltou depois de criar a conta
        nuncaAcessouDepois: 0,
        semTelefoneENuncaVoltou: 0,
        // recorte por tipo de conta
        contasGoogle: 0,
        contasGoogleSemTelefone: 0,
        anunciantes: donosComAnuncio.size,
      };

      usersSnap.forEach((doc) => {
        const u = doc.data() || {};
        r.totalUsuarios++;

        const temPerfil = temTelefoneNoPerfil(u);
        const temNoAnuncio = donosComTelefoneNoAnuncio.has(doc.id);
        const isGoogle = !!u.isGoogleUser || !!(u.photoURL || '').includes('googleusercontent');

        // "nunca voltou" = sem registro de último acesso posterior ao cadastro
        const nuncaVoltou = !u.lastAccess && !u.lastLoginAt && !u.ultimoAcesso;

        if (temPerfil) r.comTelefoneNoPerfil++;
        else {
          r.semTelefoneNoPerfil++;
          if (temNoAnuncio) r.semNoPerfilMasTemNoAnuncio++;
          else r.semTelefoneEmLugarNenhum++;
          if (nuncaVoltou) r.semTelefoneENuncaVoltou++;
        }

        if (nuncaVoltou) r.nuncaAcessouDepois++;
        if (isGoogle) {
          r.contasGoogle++;
          if (!temPerfil) r.contasGoogleSemTelefone++;
        }
      });

      const pct = (n) => (r.totalUsuarios ? Math.round((n / r.totalUsuarios) * 100) : 0);
      res.json({
        ...r,
        percentuais: {
          comTelefone: pct(r.comTelefoneNoPerfil) + '%',
          semTelefone: pct(r.semTelefoneNoPerfil) + '%',
          semTelefoneEmLugarNenhum: pct(r.semTelefoneEmLugarNenhum) + '%',
        },
      });
    } catch (err) {
      console.error('[diag-cadastros] erro:', err.message);
      res.status(500).json({ erro: err.message });
    }
  });

  console.log('[diag-cadastros] Rota registrada (/api/diag/cadastros)');
};

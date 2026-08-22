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
const { getAuth } = require('firebase-admin/auth');

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

      // Docs "fantasma": tem nome mas nao tem email — indicam doc criado por
      // merge (updateProfile nunca grava email) em vez do fluxo de cadastro.
      const fantasmas = [];
      usersSnap.forEach((doc) => {
        const u = doc.data() || {};
        if (u.email) return;
        fantasmas.push({
          // sem expor o nome: so o padrao de campos presentes
          campos: Object.keys(u).sort().join(','),
          temUid: !!u.uid,
          temCreatedAt: !!u.createdAt,
          temRole: !!u.role,
          temDocumento: !!(u.cpf || u.cnpj),
          temProfile: !!u.profile,
          criadoEm: u.createdAt && u.createdAt.toDate ? u.createdAt.toDate().toISOString() : null,
        });
      });
      // Cruza com o Firebase Auth: a conta tem e-mail LA? Se sim, o e-mail
      // existe e o problema esta so na gravacao do documento.
      const semEmailIds = [];
      usersSnap.forEach((doc) => {
        const u = doc.data() || {};
        if (!u.email) semEmailIds.push(doc.id);
      });
      const noAuth = [];
      for (const uid of semEmailIds.slice(0, 8)) {
        try {
          const rec = await getAuth().getUser(uid);
          noAuth.push({
            temEmailNoAuth: !!rec.email,
            dominioEmail: rec.email ? rec.email.split('@')[1] : null,
            provedores: (rec.providerData || []).map((p) => p.providerId),
            temNomeNoAuth: !!rec.displayName,
            criadoAuth: rec.metadata && rec.metadata.creationTime,
            ultimoLogin: rec.metadata && rec.metadata.lastSignInTime,
          });
        } catch (e) {
          noAuth.push({ erro: e.code || e.message });
        }
      }
      // Lista das contas orfas para o dono decidir a limpeza: nome, quando foi
      // criada e se realmente esta inacessivel (sem provedor no Auth).
      const orfas = [];
      for (const uid of semEmailIds) {
        const docData = usersSnap.docs.find((d) => d.id === uid);
        const u = docData ? docData.data() : {};
        let auth = null;
        try {
          const rec = await getAuth().getUser(uid);
          auth = {
            provedores: (rec.providerData || []).map((p) => p.providerId),
            temEmail: !!rec.email,
            criadoEm: rec.metadata && rec.metadata.creationTime,
            ultimoLogin: rec.metadata && rec.metadata.lastSignInTime,
          };
        } catch (e) {
          auth = { erro: e.code || e.message };
        }
        orfas.push({
          uid,
          nome: u.displayName || '(sem nome)',
          temDocumento: !!(u.cpf || u.cnpj),
          temTelefone: !!(u.profile && (u.profile.phone || (u.profile.phones && u.profile.phones.primary))),
          // seguro apagar = sem provedor de login E sem dados de negocio
          inacessivel: !!auth && Array.isArray(auth.provedores) && auth.provedores.length === 0,
          auth,
        });
      }
      // ordena: as inacessiveis (candidatas a limpeza) primeiro
      orfas.sort((a, b) => Number(b.inacessivel) - Number(a.inacessivel));
      r.docsSemEmail = { total: fantasmas.length, lista: orfas, noFirebaseAuth: noAuth };

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

  // ============================================================
  // Limpeza das contas ORFAS — POST /api/diag/limpar-orfas?token=...
  //
  // Remove contas criadas pelo bug do unlink: existem no Auth mas SEM nenhum
  // provedor de login, entao ninguem consegue acessa-las. Exige confirmacao
  // explicita e REVERIFICA cada conta na hora — nunca confia numa lista
  // pre-calculada. Guarda o que foi removido em `deleted-orphan-accounts`.
  // ============================================================
  app.post('/api/diag/limpar-orfas', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    if (!req.body || req.body.confirmar !== 'APAGAR_ORFAS') {
      return res.status(400).json({
        erro: 'Confirmacao ausente. Envie {"confirmar":"APAGAR_ORFAS"} no corpo.',
      });
    }
    const db = getDb();
    if (!db) return res.status(503).json({ erro: 'Firestore indisponivel' });

    const removidas = [];
    const preservadas = [];
    try {
      const usersSnap = await db.collection('users').get();

      for (const docSnap of usersSnap.docs) {
        const u = docSnap.data() || {};
        if (u.email) continue;                       // tem e-mail: nao e orfa
        if (u.cpf || u.cnpj) { preservadas.push({ motivo: 'tem documento' }); continue; }

        // TRAVA: reverifica no Auth agora. So apaga quem nao tem provedor algum.
        let rec = null;
        try {
          rec = await getAuth().getUser(docSnap.id);
        } catch (e) {
          // conta ja nao existe no Auth — remove so o documento residual
          await docSnap.ref.delete();
          removidas.push({ uid: docSnap.id, nome: u.displayName || null, apenasDoc: true });
          continue;
        }
        const provedores = (rec.providerData || []).map((p) => p.providerId);
        if (provedores.length > 0 || rec.email) {
          preservadas.push({ nome: rec.displayName || null, provedores, motivo: 'acessivel' });
          continue;
        }

        // Registro do que sera removido (auditoria) antes de apagar
        await db.collection('deleted-orphan-accounts').doc(docSnap.id).set({
          uid: docSnap.id,
          displayName: u.displayName || null,
          criadoEm: (rec.metadata && rec.metadata.creationTime) || null,
          removidoEm: new Date().toISOString(),
          motivo: 'conta sem provedor de login (bug do unlink no Google)',
          dadosOriginais: u,
        });

        await getAuth().deleteUser(docSnap.id);
        await docSnap.ref.delete();
        removidas.push({
          uid: docSnap.id,
          nome: u.displayName || null,
          criadaEm: (rec.metadata && rec.metadata.creationTime) || null,
        });
      }

      res.json({
        ok: true,
        removidas: removidas.length,
        preservadas: preservadas.length,
        detalheRemovidas: removidas,
        detalhePreservadas: preservadas,
      });
    } catch (err) {
      console.error('[limpar-orfas] erro:', err.message);
      res.status(500).json({ erro: err.message, removidasAntesDoErro: removidas.length });
    }
  });

  // Ficha de uma conta pelo e-mail (somente leitura) — para investigar cadastros suspeitos
  app.get('/api/diag/usuario', async (req, res) => {
    if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
      return res.sendStatus(403);
    }
    const email = String(req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ erro: 'email obrigatorio' });
    try {
      const db = getDb(); // inicializa o app admin antes de usar o Auth
      const rec = await getAuth().getUserByEmail(email);
      const doc = db ? await db.collection('users').doc(rec.uid).get() : null;
      const u = (doc && doc.exists && doc.data()) || {};
      res.json({
        uid: rec.uid,
        provedores: (rec.providerData || []).map((p) => p.providerId),
        emailVerificado: rec.emailVerified,
        criadoEm: rec.metadata && rec.metadata.creationTime,
        ultimoLogin: rec.metadata && rec.metadata.lastSignInTime,
        temDocFirestore: !!(doc && doc.exists),
        camposFirestore: Object.keys(u).sort(),
        displayName: u.displayName || rec.displayName || null,
        profileType: u.profile || u.profileType || null,
        termsAccepted: u.termsAccepted || u.acceptedTerms || null,
      });
    } catch (err) {
      // Nao existe no Auth: procura documento solto no Firestore com esse e-mail
      try {
        const db = getDb();
        const snap = await db.collection('users').where('email', '==', email).get();
        if (snap.empty) return res.status(404).json({ erro: err.message, firestore: 'nenhum doc' });
        return res.json({
          noAuth: false,
          docsFirestore: snap.docs.map((d) => ({ id: d.id, dados: d.data() })),
        });
      } catch (e2) {
        res.status(404).json({ erro: err.message, erroFirestore: e2.message });
      }
    }
  });

  console.log('[diag-cadastros] Rota registrada (/api/diag/cadastros)');
};

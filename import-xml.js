// ============================================================
// Importação de carteira via XML (padrão VivaReal/ZAP — o mesmo
// que todo CRM imobiliário brasileiro sabe gerar).
//
// Etapa 1 do plano (migração assistida): cada imóvel do feed vira um
// anúncio PRÉ-PRONTO (rascunho, isActive:false) do corretor — visível
// só para ele em Meus Anúncios. A publicação exige selecionar a
// parcela no mapa (etapas 2-4, no app).
//
// POST /api/import/xml?token=<WHATSAPP_VERIFY_TOKEN>
//   body: { ownerUid: "...", feedUrl: "https://..."  OU  xmlBase64: "...",
//           maxItens?: 500 }
//   -> { criados, atualizados, pulados, erros: [...] }
//
// Reimportar o mesmo feed é seguro: casa pelo ListingID do CRM
// (id determinístico) — atualiza preço/fotos/descrição dos rascunhos e
// NUNCA rebaixa um anúncio que o corretor já publicou.
// ============================================================

const crypto = require('crypto');
const { XMLParser } = require('fast-xml-parser');
const { getDb } = require('./firebase');

const MAX_FEED_BYTES = 20 * 1024 * 1024;
const MAX_FOTOS = 10;

function auth(req, res) {
  if (!req.query.token || req.query.token !== process.env.WHATSAPP_VERIFY_TOKEN) {
    res.sendStatus(403);
    return false;
  }
  return true;
}

const arr = (x) => (Array.isArray(x) ? x : x === undefined || x === null ? [] : [x]);
const s = (x) => (x === undefined || x === null ? '' : String(x).trim());
const num = (x) => {
  if (typeof x === 'number') return x;
  const n = parseFloat(String(x || '').replace(/[^0-9,.-]/g, '').replace(/\.(?=\d{3}(\D|$))/g, '').replace(',', '.'));
  return Number.isFinite(n) ? n : 0;
};

/** Área em HECTARES a partir de LotArea/ConstructedArea com unidade. */
function areaHa(details) {
  const candidatos = [details?.LotArea, details?.UsableArea, details?.TotalArea];
  for (const c of candidatos) {
    if (c === undefined || c === null) continue;
    const valor = typeof c === 'object' ? num(c['#text'] ?? c.text ?? c.value) : num(c);
    if (!valor) continue;
    const unidade = String((typeof c === 'object' && (c['@_unit'] || c.unit)) || 'hectares').toLowerCase();
    if (unidade.includes('hect') || unidade === 'ha') return valor;
    if (unidade.includes('acre')) return valor * 0.404686;
    if (unidade.includes('m')) return valor / 10000; // m² -> ha
    return valor; // sem unidade: assume hectare (padrão rural)
  }
  return 0;
}

/** Tipo AdGain a partir do PropertyType do feed. */
function tipo(details) {
  const t = s(details?.PropertyType).toLowerCase();
  if (t.includes('faz')) return 'fazenda';
  if (t.includes('sít') || t.includes('sit')) return 'sitio';
  if (t.includes('chác') || t.includes('chac')) return 'chacara';
  if (t.includes('haras')) return 'haras';
  if (t.includes('terreno') || t.includes('lote') || t.includes('land')) return 'terreno';
  return 'fazenda'; // rural genérico
}

function fotos(media) {
  const itens = arr(media?.Item);
  const urls = [];
  for (const it of itens) {
    const url = typeof it === 'object' ? s(it['#text'] ?? it.text) : s(it);
    const medium = typeof it === 'object' ? s(it['@_medium'] || it.medium).toLowerCase() : 'image';
    if (!url || !/^https?:\/\//i.test(url)) continue;
    if (medium && medium !== 'image') continue; // vídeos ficam de fora do MVP
    const primary = typeof it === 'object' && String(it['@_primary'] || it.primary) === 'true';
    if (primary) urls.unshift(url);
    else urls.push(url);
    if (urls.length >= MAX_FOTOS) break;
  }
  return urls;
}

function mapListing(listing, ownerUid, feedUrl) {
  const d = listing.Details || {};
  const loc = listing.Location || {};
  const titulo = s(d.Title || listing.Title) || 'Imóvel rural importado';
  const cidade = s(loc.City);
  const uf = s(loc.State?.['@_abbreviation'] || loc.State?.abbreviation || loc.State);
  const lat = num(loc.Latitude) || null;
  const lng = num(loc.Longitude) || null;
  const ha = areaHa(d);
  const preco = num(typeof d.ListPrice === 'object' ? d.ListPrice['#text'] : d.ListPrice);
  const galeria = fotos(listing.Media);
  const listingId = s(listing.ListingID) || crypto.createHash('md5').update(titulo + cidade).digest('hex').slice(0, 10);
  const agora = new Date();

  return {
    docId: 'xml' + crypto.createHash('md5').update(ownerUid + '|' + listingId).digest('hex').slice(0, 17),
    doc: {
      ownerId: ownerUid,
      title: titulo,
      description: s(d.Description),
      type: tipo(d),
      price: preco,
      totalArea: ha,
      totalAreaUnit: 'ha',
      identification: { name: titulo, title: titulo, description: s(d.Description), totalAreaInHectares: ha },
      location: {
        address: s(loc.Address),
        city: cidade,
        state: uf,
        country: 'Brasil',
        zipCode: s(loc.PostalCode),
        ...(lat && lng ? { coordinates: { lat, lng } } : {}),
      },
      media: { gallery: galeria, mainPhotoIndex: 0 },
      // Rascunho invisível ao público: só publica após vincular a parcela
      isActive: false,
      status: 'draft',
      listing: { listingStatus: 'draft' },
      integration: {
        source: 'xml-import',
        listingId,
        feedUrl: feedUrl || null,
        detailUrl: s(listing.DetailViewUrl) || null,
        contatoFeed: s(listing.ContactInfo?.Telephone || listing.ContactInfo?.Email) || null,
        importadoEm: agora,
        pendencia: 'selecionar-parcela',
      },
      updatedAt: agora,
    },
  };
}

// ============================================================
// Proxy do feed XML para o app (o navegador não consegue buscar a
// URL do CRM direto por CORS). NÃO grava nada: só baixa e devolve o
// XML; a importação acontece no cliente com o login do corretor —
// por isso funciona igual em homologação e produção.
//
// GET /api/import/proxy-feed?t=<PROXY_FEED_TOKEN>&url=<feed>
// ============================================================

// Token PÚBLICO (vai no app) — serve só para separar este endpoint dos
// endpoints sensíveis; nunca reutilizar o WHATSAPP_VERIFY_TOKEN aqui.
const PROXY_FEED_TOKEN = 'adgain-feed-proxy-2026';

// Rate limit simples por IP (protege contra abuso do proxy aberto)
const proxyHits = new Map(); // ip -> { n, desde }
function proxyRateOk(ip) {
  const agora = Date.now();
  const reg = proxyHits.get(ip) || { n: 0, desde: agora };
  if (agora - reg.desde > 60 * 60 * 1000) { reg.n = 0; reg.desde = agora; }
  reg.n++;
  proxyHits.set(ip, reg);
  if (proxyHits.size > 5000) proxyHits.clear(); // nunca crescer sem limite
  return reg.n <= 60; // 60 buscas/hora por IP
}

function urlDeFeedValida(u) {
  let parsed;
  try { parsed = new URL(u); } catch { return false; }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return false;
  const host = parsed.hostname.toLowerCase();
  // Anti-SSRF básico: nada de IPs literais, localhost ou hosts internos
  if (/^\d+\.\d+\.\d+\.\d+$/.test(host) || host.includes(':')) return false;
  if (host === 'localhost' || host.endsWith('.local') || host.endsWith('.internal') || !host.includes('.')) return false;
  return true;
}

/** Mapeia um <Imovel> do formato "Carga" (ImobiBrasil, Union…) para o doc AdGain. */
function mapImovelCarga(I, ownerUid, feedUrl) {
  const listingId = s(I.CodigoImovel);
  const tipoBase = s(I.TipoImovel), subTipo = s(I.SubTipoImovel);
  const cidade = s(I.Cidade);
  const titulo = s(I.TituloImovel) || [tipoBase || 'Imóvel rural', cidade ? `em ${cidade}` : ''].join(' ').trim();
  const t = (tipoBase + ' ' + subTipo + ' ' + titulo).toLowerCase();
  const tipoFinal = t.includes('faz') ? 'fazenda'
    : (t.includes('sít') || t.includes('sit')) ? 'sitio'
    : (t.includes('chác') || t.includes('chac')) ? 'chacara'
    : t.includes('haras') ? 'haras'
    : (t.includes('terreno') || t.includes('lote')) ? 'terreno'
    : 'fazenda';

  // Áreas do padrão Carga são m²; feeds rurais costumam mandar hectares —
  // valor grande (≥ 10.000) tratamos como m², pequeno como ha; lote é sempre m²
  // e fica EXIBIDO em m² (lotes começam em 150 m² — 0,015 ha ficaria ilegível).
  const bruto = num(I.AreaTotal) || num(I.AreaUtil);
  const ehLote = tipoFinal === 'terreno';
  const ha = ehLote ? bruto / 10000 : (bruto >= 10000 ? bruto / 10000 : bruto);

  const fotosArr = [];
  for (const f of arr(I.Fotos?.Foto)) {
    const url = s(f?.URLArquivo);
    if (!/^https?:\/\//i.test(url) || fotosArr.length >= MAX_FOTOS) continue;
    if (s(f?.Principal) === '1') fotosArr.unshift(url);
    else fotosArr.push(url);
  }

  const lat = num(I.Latitude) || null, lng = num(I.Longitude) || null;
  const agora = new Date();
  return {
    docId: 'xml' + crypto.createHash('md5').update(ownerUid + '|' + listingId).digest('hex').slice(0, 17),
    doc: {
      ownerId: ownerUid,
      title: titulo,
      description: s(I.Observacao),
      type: tipoFinal,
      price: num(I.PrecoVenda),
      totalArea: ehLote ? bruto : ha,
      totalAreaUnit: ehLote ? 'm2' : 'ha',
      identification: { name: titulo, title: titulo, description: s(I.Observacao), totalAreaInHectares: ha },
      location: {
        address: s(I.Endereco), city: cidade, state: s(I.UF).toUpperCase().slice(0, 2),
        country: 'Brasil', zipCode: s(I.CEP),
        ...(lat && lng ? { coordinates: { lat, lng } } : {}),
      },
      media: { gallery: fotosArr, mainPhotoIndex: 0 },
      isActive: false,
      status: 'draft',
      listing: { listingStatus: 'draft' },
      integration: {
        source: 'xml-import', listingId, feedUrl: feedUrl || null,
        importadoEm: agora, pendencia: 'selecionar-parcela',
      },
      updatedAt: agora,
    },
  };
}

module.exports = function registerImportXml(app) {
  app.get('/api/import/proxy-feed', async (req, res) => {
    if (req.query.t !== PROXY_FEED_TOKEN) return res.sendStatus(403);
    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.ip || '?';
    if (!proxyRateOk(ip)) return res.status(429).json({ error: 'Muitas buscas — tente em alguns minutos.' });

    const url = String(req.query.url || '');
    if (!urlDeFeedValida(url)) {
      return res.status(400).json({ error: 'URL de feed inválida.' });
    }

    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 25000);
      const r = await fetch(url, {
        signal: ctrl.signal,
        redirect: 'follow',
        headers: { 'user-agent': 'AdGain-FeedImporter/1.0', accept: 'application/xml, text/xml, */*' },
      });
      clearTimeout(timer);
      if (!r.ok) return res.status(502).json({ error: `O CRM respondeu ${r.status} ao buscar o feed.` });

      const texto = await r.text();
      if (texto.length > MAX_FEED_BYTES) {
        return res.status(413).json({ error: 'Feed acima de 20MB.' });
      }
      const inicio = texto.trimStart().slice(0, 200).toLowerCase();
      if (!inicio.startsWith('<')) {
        return res.status(422).json({ error: 'A URL não devolveu um XML (confira se é o link do feed do CRM).' });
      }
      res.type('text/xml').send(texto);
    } catch (e) {
      const msg = e?.name === 'AbortError' ? 'Tempo esgotado ao buscar o feed (25s).' : 'Não foi possível baixar o feed.';
      res.status(504).json({ error: msg });
    }
  });

  app.post('/api/import/xml', async (req, res) => {
    if (!auth(req, res)) return;
    const db = getDb();
    if (!db) return res.status(500).json({ error: 'Firestore indisponível' });

    const { ownerUid, feedUrl, xmlBase64, maxItens } = req.body || {};
    if (!ownerUid) return res.status(400).json({ error: 'ownerUid obrigatório' });
    if (!feedUrl && !xmlBase64) return res.status(400).json({ error: 'feedUrl ou xmlBase64 obrigatório' });

    // dono precisa existir (evita importar para uid digitado errado)
    const dono = await db.collection('users').doc(String(ownerUid)).get();
    if (!dono.exists) return res.status(404).json({ error: 'ownerUid não encontrado em users' });

    let xml;
    try {
      if (xmlBase64) {
        xml = Buffer.from(String(xmlBase64), 'base64').toString('utf8');
      } else {
        const r = await fetch(String(feedUrl), { redirect: 'follow' });
        if (!r.ok) return res.status(502).json({ error: `feed respondeu HTTP ${r.status}` });
        const buf = Buffer.from(await r.arrayBuffer());
        if (buf.length > MAX_FEED_BYTES) return res.status(413).json({ error: 'feed maior que 20MB' });
        xml = buf.toString('utf8');
      }
    } catch (err) {
      return res.status(502).json({ error: 'falha ao obter o feed: ' + err.message });
    }

    let raiz;
    try {
      const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', trimValues: true });
      raiz = parser.parse(xml);
    } catch (err) {
      return res.status(400).json({ error: 'XML inválido: ' + err.message });
    }

    let formato = 'listingdatafeed';
    let listings = arr(
      raiz?.ListingDataFeed?.Listings?.Listing ??
      raiz?.listingDataFeed?.listings?.listing ??
      raiz?.Listings?.Listing
    );
    if (!listings.length) {
      // Formato "Carga de Imóveis" (<Carga><Imoveis><Imovel>) — ImobiBrasil, Union…
      const imoveis = arr(raiz?.Carga?.Imoveis?.Imovel ?? raiz?.carga?.imoveis?.imovel);
      if (imoveis.length) { listings = imoveis; formato = 'carga'; }
    }
    if (!listings.length) return res.status(400).json({ error: 'nenhum <Listing> (VivaReal/ZAP) nem <Imovel> (Carga) encontrado no feed' });

    const teto = Math.min(Number(maxItens) || 500, 500);
    const resultado = { criados: 0, atualizados: 0, pulados: 0, urbanosIgnorados: 0, erros: [] };

    // REGRA DO QUE ENTRA: rurais (fazenda, sítio, chácara, haras…) e
    // terrenos/lotes (urbanos ou rurais) entram; casa/apartamento/comercial
    // ficam de fora. Sem tipo declarado, entra.
    const normalizar = (t) => String(t || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
    const entraNaAdGain = (listing) => {
      const d = listing?.Details || listing || {};
      const tipoTxt = formato === 'carga'
        ? normalizar(s(listing?.TipoImovel) + ' ' + s(listing?.SubTipoImovel))
        : normalizar(d.PropertyType);
      if (!tipoTxt.trim()) return true;
      const titulo = formato === 'carga' ? s(listing?.TituloImovel) : s(d.Title);
      const texto = tipoTxt + ' ' + normalizar(titulo);
      return /(rural|fazend|sit[ie]|chac|haras|rancho|agricol|farm|gleba)/.test(texto)
        || /(terreno|lote|land)/.test(tipoTxt);
    };

    for (const listing of listings.slice(0, teto)) {
      // Registro sem código (comum em feed Carga vazio): ignora
      const codigo = formato === 'carga' ? s(listing?.CodigoImovel) : s(listing?.ListingID ?? listing?.ListingId);
      if (!codigo) { resultado.pulados++; continue; }
      if (!entraNaAdGain(listing)) { resultado.urbanosIgnorados++; continue; }
      try {
        const { docId, doc } = formato === 'carga'
          ? mapImovelCarga(listing, String(ownerUid), feedUrl ? String(feedUrl) : null)
          : mapListing(listing, String(ownerUid), feedUrl ? String(feedUrl) : null);
        const ref = db.collection('properties').doc(docId);
        const atual = await ref.get();
        if (!atual.exists) {
          await ref.set({ ...doc, createdAt: new Date() });
          resultado.criados++;
        } else {
          const st = String(atual.data().status || '').toLowerCase();
          if (st !== 'draft') { resultado.pulados++; continue; } // já publicado: não rebaixa
          const { isActive, status, listing: _l, ...atualizaveis } = doc;
          await ref.set(atualizaveis, { merge: true });
          resultado.atualizados++;
        }
      } catch (err) {
        resultado.erros.push(String(err.message).slice(0, 120));
      }
    }

    console.log('[import-xml]', ownerUid, JSON.stringify(resultado).slice(0, 200));
    res.json({ ...resultado, totalNoFeed: listings.length });
  });

  console.log('[import-xml] Rota /api/import/xml registrada');
};

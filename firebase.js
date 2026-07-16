// ============================================================
// Acesso ao Firestore do projeto adgain-sistemas (Firebase Admin).
//
// Credencial (uma das duas):
//   FIREBASE_SERVICE_ACCOUNT        JSON completo da service account
//                                   (colado como env var no Render)
//   GOOGLE_APPLICATION_CREDENTIALS  caminho do arquivo JSON (uso local)
//
// getDb() retorna null se não houver credencial — quem chama decide
// o fallback (o bot degrada para as respostas estáticas).
// ============================================================

const { initializeApp, applicationDefault, cert, getApps } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

let db = null;
let initTried = false;

function getDb() {
  if (db || initTried) return db;
  initTried = true;
  try {
    if (!getApps().length) {
      if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        initializeApp({ credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
      } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
        initializeApp({ credential: applicationDefault() });
      } else {
        console.warn('[firebase] Sem credencial (FIREBASE_SERVICE_ACCOUNT) — Firestore indisponível');
        return null;
      }
    }
    db = getFirestore();
    console.log('[firebase] Firestore inicializado');
  } catch (err) {
    console.error('[firebase] Falha ao inicializar:', err.message);
  }
  return db;
}

module.exports = { getDb };

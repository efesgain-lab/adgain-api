// ============================================================
// Backfill de users.phoneDigits — índice de telefones para o bot WhatsApp.
//
// Gera, para cada usuário, as variantes só-dígitos dos telefones do perfil
// (profile.phones.primary/secondary), com e sem o 9º dígito de celular BR,
// e grava no campo raiz phoneDigits (array). O bot busca por
// array-contains com o wa_id que chega no webhook.
//
// Uso (local):
//   set GOOGLE_APPLICATION_CREDENTIALS=C:\caminho\service-account.json
//   node scripts/backfill-phone-digits.js          (dry-run: só mostra)
//   node scripts/backfill-phone-digits.js --write  (grava de verdade)
// ============================================================

const { initializeApp, applicationDefault } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

initializeApp({ credential: applicationDefault() });
const db = getFirestore();

const WRITE = process.argv.includes('--write');

function variants(code, number) {
  let digits = (String(code || '') + String(number || '')).replace(/\D+/g, '');
  if (!digits) return [];
  // sem código de país e com DDD (10-11 dígitos): assume Brasil
  if (digits.length === 10 || digits.length === 11) digits = '55' + digits;
  if (digits.length < 12) return [];
  const out = new Set([digits]);
  // celular BR 13 dígitos (55 + DDD + 9XXXXXXXX): variante sem o 9
  if (digits.length === 13 && digits.startsWith('55') && digits[4] === '9') {
    out.add(digits.slice(0, 4) + digits.slice(5));
  }
  // 12 dígitos (55 + DDD + XXXXXXXX): variante com o 9
  if (digits.length === 12 && digits.startsWith('55')) {
    out.add(digits.slice(0, 4) + '9' + digits.slice(4));
  }
  return [...out];
}

(async () => {
  const snap = await db.collection('users').get();
  let comTelefone = 0;
  let atualizados = 0;

  for (const doc of snap.docs) {
    const d = doc.data() || {};
    const phones = (d.profile && d.profile.phones) || {};
    const digits = [
      ...variants(phones.primary && phones.primary.code, phones.primary && phones.primary.number),
      ...variants(phones.secondary && phones.secondary.code, phones.secondary && phones.secondary.number),
    ];
    const unico = [...new Set(digits)];
    if (!unico.length) continue;
    comTelefone++;

    const atual = Array.isArray(d.phoneDigits) ? d.phoneDigits : [];
    const igual = atual.length === unico.length && unico.every((v) => atual.includes(v));
    if (igual) continue;

    console.log(`${doc.id}: ${JSON.stringify(unico)}${WRITE ? '' : ' (dry-run)'}`);
    if (WRITE) {
      await doc.ref.update({ phoneDigits: unico });
      atualizados++;
    }
  }

  console.log(
    `\nTotal: ${snap.size} usuários | ${comTelefone} com telefone | ` +
      (WRITE ? `${atualizados} atualizados` : 'dry-run (use --write para gravar)')
  );
  process.exit(0);
})().catch((err) => {
  console.error('Backfill falhou:', err);
  process.exit(1);
});

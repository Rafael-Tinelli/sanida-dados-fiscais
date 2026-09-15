<?php
/**
 * Fonte versionada da página H27.
 *
 * A divisão em partes preserva o HTML/PHP efetivamente publicado enquanto
 * mantém a migração C6.4 revisável junto do runtime fiscal.
 *
 * Manifesto estrutural C6.4 (os gates também validam os arquivos em parts/):
 * /financas/calculadoras/assets/folha-core.js
 * /financas/calculadoras/assets/folha-thirteenth.js
 * /financas/calculadoras/assets/decimo-terceiro.js
 * name="data_quitacao"
 * name="adiantamento_pago"
 * name="salario_mes_anterior"
 * name="data_adiantamento"
 * name="admissao_ano"
 * data-row="status-adiantamento"
 * data-row="base-ir"
 * data-row="ir-antes-reducao"
 * data-row="renda-redutor"
 * data-row="referencia"
 * data-row="release-id"
 * Release fiscal usada
 */
$h27_parts = [
    '01-head-hero.php',
    '02-calculator.php',
    '03-guide.php',
    '04-faq-footer.php',
];

foreach ($h27_parts as $h27_part) {
    include __DIR__ . '/parts/' . $h27_part;
}

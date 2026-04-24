const PDFDocument = require('pdfkit')
const { Readable } = require('stream');

class ReportService {
  /**
   * Generate PDF report from analysis results
   * @param {Object} data - Report data containing analyses, municipio, geojson, generatedAt
   * @returns {Promise<Buffer>} - PDF as buffer
   */
  static async generatePDF(data) {
    return new Promise((resolve, reject) => {
      try {
        const doc = new PDFDocument({
          size: 'A4',
          margin: 40,
          bufferPages: true,
        });

        const chunks = [];

        doc.on('data', chunk => chunks.push(chunk));
        doc.on('end', () => resolve(Buffer.concat(chunks)));
        doc.on('error', reject);

        // Generate content
        this._addCoverPage(doc, data);
        this._addTableOfContents(doc);
        this._addAnalyses(doc, data);
        this._addLegalDisclaimer(doc);

        doc.end();
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Add cover page
   */
  static _addCoverPage(doc, data) {
    // Background color
    doc.rect(0, 0, doc.page.width, doc.page.height).fill('#f0f4f8');

    // Reset to black text
    doc.fillColor('black');

    // Logo/Title area
    doc.fontSize(28).font('Helvetica-Bold');
    doc.text('ADGAIN', 40, 100, { align: 'center' });

    doc.fontSize(14).font('Helvetica');
    doc.text('Análises Geoespaciais Integradas', 40, 140, { align: 'center' });

    doc.fontSize(12);
    doc.text('Relatório de Análises Espaciais', 40, 180, { align: 'center' });

    // Municipio info
    doc.fontSize(11).font('Helvetica-Bold');
    doc.text(`Município: ${data.municipio || 'Desconhecido'}`, 40, 240);
    doc.fontSize(11).font('Helvetica');
    doc.text(`Gerado em: ${data.generatedAt.toLocaleString('pt-BR')}`, 40, 260);

    // Area info
    const analyses = data.analyses || {};
    if (analyses['9.1_fundiaria'] && analyses['9.1_fundiaria'].sigef) {
      doc.text(
        `Total de parcelas SIGEF: ${analyses['9.1_fundiaria'].sigef.length}`,
        40,
        280
      );
    }

    // Divider line
    doc.moveTo(40, 320).lineTo(doc.page.width - 40, 320).stroke();

    // Key findings
    doc.fontSize(12).font('Helvetica-Bold');
    doc.text('Principais Características:', 40, 350);

    doc.fontSize(11).font('Helvetica');
    let y = 375;

    // Area hectares
    if (data.analyses && data.analyses['9.3_solo']) {
      doc.text(
        `• Área de solo mapeada: ${this._getAreaData(data.analyses)} hectares`,
        40,
        y
      );
      y += 20;
    }

    // Bioma
    if (data.analyses && data.analyses['9.4_bioma'] && data.analyses['9.4_bioma'].data.length > 0) {
      const bioma = data.analyses['9.4_bioma'].data[0];
      doc.text(`• Bioma principal: ${bioma.nome}`, 40, y);
      y += 20;
    }

    // UCs
    if (data.analyses && data.analyses['9.9_ucs'] && data.analyses['9.9_ucs'].data.length > 0) {
      doc.text(
        `• Unidades de Conservação: ${data.analyses['9.9_ucs'].data.length} identificadas`,
        40,
        y
      );
      y += 20;
    }

    // Hidrografia
    if (data.analyses && data.analyses['9.10_hidrografia']) {
      const hidro = data.analyses['9.10_hidrografia'];
      doc.text(
        `• Cursos de água: ${hidro.cursos_agua_count}`,
        40,
        y
      );
      y += 20;
    }

    // Footer
    doc.fontSize(9).fillColor('#666');
    doc.text('Este é um documento gerado automaticamente pela plataforma AdGain.', 40, doc.page.height - 60);
    doc.text('As análises são baseadas em dados geoespaciais públicos e devem ser validadas.', 40, doc.page.height - 45);

    doc.addPage();
  }

  /**
   * Add table of contents
   */
  static _addTableOfContents(doc) {
    doc.fontSize(14).font('Helvetica-Bold').fillColor('black');
    doc.text('Sumário', 40, 40);

    const toc = [
      '1. Análise Fundiária',
      '2. Registral',
      '3. Solo (Pedologia)',
      '4. Bioma',
      '5. Geologia',
      '6. Mineração',
      '7. Embargos Ambientais',
      '8. Terras Indígenas',
      '9. Unidades de Conservação',
      '10. Hidrografia',
      '11. Altitude',
      '12. Carbono',
      '13. CAR (Cadastro Ambiental Rural)',
      '14. Análises Adicionais',
    ];

    doc.fontSize(11).font('Helvetica');
    let y = 80;
    toc.forEach(item => {
      doc.text(item, 40, y);
      y += 20;
    });

    doc.addPage();
  }

  /**
   * Add all analyses sections
   */
  static _addAnalyses(doc, data) {
    const analyses = data.analyses || {};

    // 9.1 Fundiária
    this._addSection(doc, '9.1 Análise Fundiária', analyses['9.1_fundiaria']);

    // 9.2 Registral
    this._addSection(doc, '9.2 Registral', analyses['9.2_registral']);

    // 9.3 Solo - with colored map
    this._addSoloSection(doc, analyses['9.3_solo'], data.soloGeoms, data.geojson);

    // 9.4 Bioma
    this._addSection(doc, '9.4 Bioma', analyses['9.4_bioma']);

    // 9.5 Geologia
    this._addSection(doc, '9.5 Geologia', analyses['9.5_geologia']);

    // 9.6 Mineração
    this._addSection(doc, '9.6 Mineração', analyses['9.6_mineracao']);

    // 9.7 Embargos
    this._addSection(doc, '9.7 Embargos Ambientais', analyses['9.7_embargos']);

    // 9.8 Terras Indígenas
    this._addSection(doc, '9.8 Terras Indígenas', analyses['9.8_terras_indigenas']);

    // 9.9 UCs
    this._addSection(doc, '9.9 Unidades de Conservação', analyses['9.9_ucs']);

    // 9.10 Hidrografia
    this._addSection(doc, '9.10 Hidrografia', analyses['9.10_hidrografia']);

    // 9.11 Altitude
    this._addSection(doc, '9.11 Altitude', analyses['9.11_altitude']);

    // 9.12 Carbono
    this._addSection(doc, '9.12 Carbono do Solo', analyses['9.12_carbono']);

    // 9.13 CAR
    this._addSection(doc, '9.13 CAR', analyses['9.13_car']);

    // 9.14 Análises Adicionais
    this._addSection(doc, '9.14 Análises Adicionais', analyses['9.14_analises_adicionais']);
  }

  /**
   * Add a single analysis section
   */
  static _addSection(doc, title, data) {
    // Check if we need a new page
    if (doc.y > doc.page.height - 100) {
      doc.addPage();
    }

    // Section title
    doc.fontSize(12).font('Helvetica-Bold').fillColor('#1a3a52');
    doc.text(title, 40, doc.y);
    doc.moveTo(40, doc.y + 5).lineTo(doc.page.width - 40, doc.y + 5).stroke();
    doc.y += 25;

    doc.fillColor('black').font('Helvetica');

    if (!data) {
      doc.fontSize(10).text('Sem dados disponíveis', 40, doc.y);
      doc.y += 15;
      return;
    }

    // Handle different section types
    if (title === '9.1 Análise Fundiária') {
      this._addFundiariaSection(doc, data);
    } else if (title === '9.2 Registral') {
      this._addRegistralSection(doc, data);
    } else if (title === '9.3 Solo (Pedologia)' || title === '9.4 Bioma' || title === '9.5 Geologia') {
      this._addPercentualSection(doc, data, title);
    } else if (title === '9.6 Mineração') {
      this._addMineracaoSection(doc, data);
    } else if (title === '9.10 Hidrografia') {
      this._addHidrografiaSection(doc, data);
    } else if (title === '9.11 Altitude') {
      this._addAltitudeSection(doc, data);
    } else if (title === '9.12 Carbono do Solo') {
      this._addCarbnoSection(doc, data);
    } else if (title === '9.13 CAR') {
      this._addCARSection(doc, data);
    } else if (title === '9.14 Análises Adicionais') {
      this._addAnalisisAdicionaisSection(doc, data);
    } else {
      // Generic table for data arrays
      this._addGenericDataSection(doc, data);
    }

    doc.y += 20;
  }

  /**
   * Add Fundiária section
   */
  static _addFundiariaSection(doc, data) {
    doc.fontSize(10).font('Helvetica-Bold');

    if (data.sigef && data.sigef.length > 0) {
      doc.text('SIGEF (Mato Grosso):', 40, doc.y);
      doc.font('Helvetica');

      data.sigef.forEach(item => {
        doc.fontSize(9).text(
          `• ${item.numero} - ${item.area_hectares} hectares`,
          50,
          doc.y
        );
      });
    }

    if (data.snci && data.snci.length > 0) {
      doc.fontSize(10).font('Helvetica-Bold');
      doc.text('SNCI (Mato Grosso):', 40, doc.y + 10);
      doc.font('Helvetica').fontSize(9);

      data.snci.forEach(item => {
        doc.text(
          `• ${item.numero} - ${item.area_hectares} hectares`,
          50,
          doc.y
        );
      });
    }

    if (!data.sigef || data.sigef.length === 0) {
      if (!data.snci || data.snci.length === 0) {
        doc.fontSize(10).text('Nenhuma parcela fundiária registrada', 40, doc.y);
      }
    }
  }

  /**
   * Add Registral section
   */
  static _addRegistralSection(doc, data) {
    if (data.message) {
      doc.fontSize(10).text(data.message, 40, doc.y);
    }

    if (data.serventias && data.serventias.length > 0) {
      doc.fontSize(10).font('Helvetica-Bold').text('Serventias Disponíveis:', 40, doc.y + 10);
      doc.font('Helvetica').fontSize(9);

      data.serventias.forEach(item => {
        doc.text(`• ${item.nome} (Cartório ${item.cartorio_numero})`, 50, doc.y);
      });
    }
  }

  /**
   * Add Solo section with colored map + table
   */
  static _addSoloSection(doc, data, soloGeoms, geojson) {
    if (doc.y > doc.page.height - 100) doc.addPage();

    doc.fontSize(12).font('Helvetica-Bold').fillColor('#1a3a52');
    doc.text('9.3 Solo (Pedologia)', 40, doc.y);
    doc.moveTo(40, doc.y + 5).lineTo(doc.page.width - 40, doc.y + 5).stroke();
    doc.y += 25;
    doc.fillColor('black').font('Helvetica');

    if (!data) {
      doc.fontSize(10).text('Sem dados disponíveis', 40, doc.y);
      doc.y += 15;
      return;
    }

    // Draw colored soil map
    if (soloGeoms && soloGeoms.length > 0) {
      this._drawSoloMap(doc, soloGeoms, geojson);
      doc.y += 10;
    }

    // Draw percentual table below the map
    this._addPercentualSection(doc, data, '9.3 Solo (Pedologia)');
  }

  /**
   * Return a distinct color for each Brazilian soil order (SiBCS)
   */
  static _getSoilColor(nome) {
    if (!nome) return '#AAAAAA';
    const u = nome.toUpperCase();
    if (u.includes('LATOSSOLO'))   return '#E8A838';
    if (u.includes('ARGISSOLO'))   return '#C87032';
    if (u.includes('NITOSSOLO'))   return '#A0522D';
    if (u.includes('CAMBISSOLO'))  return '#DEB887';
    if (u.includes('GLEISSOLO'))   return '#6B8E9F';
    if (u.includes('NEOSSOLO'))    return '#F4D03F';
    if (u.includes('ESPODOSSOLO')) return '#85929E';
    if (u.includes('PLINTOSSOLO')) return '#C0392B';
    if (u.includes('VERTISSOLO'))  return '#839192';
    if (u.includes('CHERNOSSOLO')) return '#2C3E50';
    if (u.includes('LUVISSOLO'))   return '#D35400';
    if (u.includes('PLANOSSOLO'))  return '#76B7D0';
    if (u.includes('ORGANOSSOLO')) return '#1A5276';
    return '#AAAAAA';
  }

  /**
   * Build a projector function: [lon,lat] → [pdfX, pdfY]
   */
  static _makeProjector(bbox, mapX, mapY, mapW, mapH) {
    const [minX, minY, maxX, maxY] = bbox;
    const rangeX = maxX - minX || 0.001;
    const rangeY = maxY - minY || 0.001;
    const scaleX = mapW / rangeX;
    const scaleY = mapH / rangeY;
    const scale = Math.min(scaleX, scaleY);
    const offX = mapX + (mapW - rangeX * scale) / 2;
    const offY = mapY + mapH - (mapH - rangeY * scale) / 2;
    return ([lon, lat]) => [offX + (lon - minX) * scale, offY - (lat - minY) * scale];
  }

  /**
   * Collect all [lon,lat] coordinates from a GeoJSON geometry or feature
   */
  static _getCoords(geom) {
    if (!geom) return [];
    const g = geom.geometry || geom; // unwrap Feature
    const out = [];
    const walk = (c) => {
      if (!Array.isArray(c)) return;
      if (typeof c[0] === 'number') { out.push(c); return; }
      c.forEach(walk);
    };
    if (g.coordinates) walk(g.coordinates);
    else if (g.geometries) g.geometries.forEach(sub => out.push(...this._getCoords(sub)));
    return out;
  }

  /**
   * Draw GeoJSON geometry rings onto the current pdfkit path (no fill/stroke called here)
   */
  static _drawGeomRings(doc, geom, projFn) {
    if (!geom) return;
    const g = geom.geometry || geom;

    const drawRing = (ring) => {
      if (!ring || ring.length < 2) return;
      const [x0, y0] = projFn(ring[0]);
      doc.moveTo(x0, y0);
      for (let i = 1; i < ring.length; i++) {
        const [px, py] = projFn(ring[i]);
        doc.lineTo(px, py);
      }
      doc.closePath();
    };

    const drawPoly = (rings) => rings.forEach(drawRing);

    if (g.type === 'Polygon') {
      drawPoly(g.coordinates);
    } else if (g.type === 'MultiPolygon') {
      g.coordinates.forEach(drawPoly);
    } else if (g.type === 'GeometryCollection') {
      g.geometries.forEach(sub => this._drawGeomRings(doc, sub, projFn));
    }
  }

  /**
   * Draw a colored soil map with property outline and legend
   */
  static _drawSoloMap(doc, soloGeoms, geojson) {
    // Parse property geometry
    let propGeom = null;
    try {
      const gj = typeof geojson === 'string' ? JSON.parse(geojson) : geojson;
      if (gj) {
        if (gj.type === 'FeatureCollection') propGeom = gj.features[0]?.geometry;
        else if (gj.type === 'Feature') propGeom = gj.geometry;
        else propGeom = gj;
      }
    } catch (e) { /* ignore */ }

    // Parse soil geometries
    const soilParsed = [];
    for (const row of soloGeoms) {
      try {
        const g = typeof row.geom_json === 'string' ? JSON.parse(row.geom_json) : row.geom_json;
        if (g) soilParsed.push({ nome: row.nome || 'Solo', percentual: row.percentual || 0, geom: g });
      } catch (e) { /* skip invalid geoms */ }
    }

    if (soilParsed.length === 0 && !propGeom) return;

    // Compute bounding box
    const allCoords = [];
    soilParsed.forEach(s => allCoords.push(...this._getCoords(s.geom)));
    if (propGeom) allCoords.push(...this._getCoords(propGeom));
    if (allCoords.length === 0) return;

    let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;
    allCoords.forEach(([lon, lat]) => {
      if (lon < minLon) minLon = lon;
      if (lat < minLat) minLat = lat;
      if (lon > maxLon) maxLon = lon;
      if (lat > maxLat) maxLat = lat;
    });
    const padLon = (maxLon - minLon) * 0.08 || 0.001;
    const padLat = (maxLat - minLat) * 0.08 || 0.001;
    minLon -= padLon; maxLon += padLon;
    minLat -= padLat; maxLat += padLat;

    const mapX = 40;
    const mapW = doc.page.width - 80;
    const mapH = 220;

    if (doc.y + mapH + 60 > doc.page.height - 40) doc.addPage();

    const mapY = doc.y;
    const projFn = this._makeProjector([minLon, minLat, maxLon, maxLat], mapX, mapY, mapW, mapH);

    // Background
    doc.save();
    doc.rect(mapX, mapY, mapW, mapH).fillColor('#e8f4f8').fill();
    doc.restore();

    // Draw each soil polygon filled with its color
    soilParsed.forEach(s => {
      const color = this._getSoilColor(s.nome);
      doc.save();
      this._drawGeomRings(doc, s.geom, projFn);
      doc.fillColor(color).fill();
      doc.restore();

      // White border between polygons
      doc.save();
      this._drawGeomRings(doc, s.geom, projFn);
      doc.strokeColor('#ffffff').lineWidth(0.5).stroke();
      doc.restore();
    });

    // Draw property outline on top
    if (propGeom) {
      doc.save();
      this._drawGeomRings(doc, propGeom, projFn);
      doc.strokeColor('#000000').lineWidth(1.5).stroke();
      doc.restore();
    }

    // Map frame border
    doc.save();
    doc.rect(mapX, mapY, mapW, mapH).strokeColor('#999999').lineWidth(0.5).stroke();
    doc.restore();

    doc.y = mapY + mapH + 6;

    // Legend
    const legendBoxSize = 10;
    const legendColW = 185;
    const legendCols = Math.max(1, Math.floor(mapW / legendColW));
    let lx = mapX;
    let ly = doc.y;

    doc.fontSize(7.5).font('Helvetica');
    soilParsed.forEach((item, i) => {
      if (i > 0 && i % legendCols === 0) {
        ly += 16;
        lx = mapX;
      } else if (i > 0) {
        lx += legendColW;
      }
      // Color box
      doc.save();
      doc.rect(lx, ly, legendBoxSize, legendBoxSize).fillColor(this._getSoilColor(item.nome)).fill();
      doc.restore();
      doc.save();
      doc.rect(lx, ly, legendBoxSize, legendBoxSize).strokeColor('#666').lineWidth(0.3).stroke();
      doc.restore();
      // Label
      doc.fillColor('black').text(
        `${item.nome} (${item.percentual}%)`.substring(0, 30),
        lx + legendBoxSize + 3,
        ly + 1,
        { width: legendColW - legendBoxSize - 6, lineBreak: false }
      );
    });

    doc.y = ly + 18;
  }

  /**
   * Add percentual-based section (Solo, Bioma, Geologia)
   */
  static _addPercentualSection(doc, data, title) {
    if (!data.data || data.data.length === 0) {
      doc.fontSize(10).text('Nenhum dado disponível', 40, doc.y);
      return;
    }

    doc.fontSize(10).font('Helvetica-Bold');
    const cols = [
      { label: 'Classe', width: 300 },
      { label: 'Percentual', width: 100 },
    ];

    this._addTable(doc, cols, data.data.map(row => [
      row.nome || 'N/A',
      `${row.percentual || 0}%`,
    ]));
  }

  /**
   * Add Mineração section
   */
  static _addMineracaoSection(doc, data) {
    doc.fontSize(10).font('Helvetica-Bold');

    if (data.processes && data.processes.length > 0) {
      doc.text('Processos ANM:', 40, doc.y);
      doc.font('Helvetica').fontSize(9);

      data.processes.slice(0, 5).forEach(item => {
        doc.text(`• ${item.numero_processo} (${item.tipo_processo}) - ${item.substancia}`, 50, doc.y);
      });

      if (data.processes.length > 5) {
        doc.text(`... e mais ${data.processes.length - 5} processos`, 50, doc.y);
      }
    }

    if (data.occurrences && data.occurrences.length > 0) {
      doc.fontSize(10).font('Helvetica-Bold');
      doc.text('Ocorrências Minerais:', 40, doc.y + 10);
      doc.font('Helvetica').fontSize(9);

      data.occurrences.slice(0, 5).forEach(item => {
        doc.text(`• ${item.nome} (${item.substancia})`, 50, doc.y);
      });

      if (data.occurrences.length > 5) {
        doc.text(`... e mais ${data.occurrences.length - 5} ocorrências`, 50, doc.y);
      }
    }

    if ((!data.processes || data.processes.length === 0) &&
        (!data.occurrences || data.occurrences.length === 0)) {
      doc.fontSize(10).text('Nenhuma atividade de mineração identificada', 40, doc.y);
    }
  }

  /**
   * Add Hidrografia section
   */
  static _addHidrografiaSection(doc, data) {
    doc.fontSize(10);

    if (data.bacias && data.bacias.length > 0) {
      doc.font('Helvetica-Bold').text('Bacias Hidrográficas:', 40, doc.y);
      doc.font('Helvetica');

      data.bacias.forEach(item => {
        doc.text(`• ${item.nome} (Nível ${item.nivel})`, 50, doc.y);
      });
    }

    doc.font('Helvetica-Bold');
    doc.text(`Cursos de Água: ${data.cursos_agua_count || 0}`, 40, doc.y + 10);

    doc.font('Helvetica');
    if (data.rica_em_agua) {
      doc.fillColor('#27ae60');
      doc.text('Status: Rica em água', 50, doc.y + 10);
      doc.fillColor('black');
    } else {
      doc.fillColor('#e74c3c');
      doc.text('Status: Baixo volume de água', 50, doc.y + 10);
      doc.fillColor('black');
    }
  }

  /**
   * Add Altitude section
   */
  static _addAltitudeSection(doc, data) {
    doc.fontSize(10).font('Helvetica');

    if (data.min_m) {
      doc.text(`Altitude Mínima: ${data.min_m} m`, 40, doc.y);
    }
    if (data.max_m) {
      doc.text(`Altitude Máxima: ${data.max_m} m`, 40, doc.y);
    }
    if (data.media_m) {
      doc.text(`Altitude Média: ${data.media_m} m`, 40, doc.y);
    }
    if (data.ponto_m) {
      doc.text(`Altitude do Centroide: ${data.ponto_m} m`, 40, doc.y);
    }

    if (!data.min_m && !data.max_m) {
      doc.text('Dados de altitude não disponíveis', 40, doc.y);
    }
  }

  /**
   * Add Carbono section
   */
  static _addCarbnoSection(doc, data) {
    doc.fontSize(10).font('Helvetica');

    if (data.total_toneladas) {
      doc.text(`Carbono do Solo: ${data.total_toneladas} toneladas`, 40, doc.y);
    } else {
      doc.text('Dados de carbono não disponíveis', 40, doc.y);
    }
  }

  /**
   * Add CAR section
   */
  static _addCARSection(doc, data) {
    doc.fontSize(10).font('Helvetica');

    if (data.area_imovel && data.area_imovel.length > 0) {
      doc.text('Imóveis CAR:', 40, doc.y);
      data.area_imovel.forEach(item => {
        doc.fontSize(9).text(
          `• ${item.numero_imovel}: ${item.area_hectares} hectares`,
          50,
          doc.y
        );
      });
    }

    doc.fontSize(10).font('Helvetica-Bold');
    doc.text('Áreas Legais:', 40, doc.y + 10);
    doc.font('Helvetica').fontSize(9);
    doc.text(`• APP: ${data.app_area_hectares} hectares`, 50, doc.y);
    doc.text(`• Reserva Legal: ${data.reserva_legal_hectares} hectares`, 50, doc.y);
    doc.text(`• Vegetação Nativa: ${data.vegetacao_nativa_hectares} hectares`, 50, doc.y);
  }

  /**
   * Add Análises Adicionais section
   */
  static _addAnalisisAdicionaisSection(doc, data) {
    doc.fontSize(10).font('Helvetica-Bold');

    if (data.geologia) {
      doc.text('Geologia:', 40, doc.y);
      doc.font('Helvetica').fontSize(9);

      if (data.geologia.pontos && data.geologia.pontos.length > 0) {
        doc.text(`• Pontos geológicos: ${data.geologia.pontos.length}`, 50, doc.y);
      }
      if (data.geologia.linhas_falha && data.geologia.linhas_falha.length > 0) {
        doc.text(`• Linhas de falha: ${data.geologia.linhas_falha.length}`, 50, doc.y);
      }
      if (data.geologia.linhas_fratura && data.geologia.linhas_fratura.length > 0) {
        doc.text(`• Linhas de fratura: ${data.geologia.linhas_fratura.length}`, 50, doc.y);
      }
    }

    if (data.tectonicas && data.tectonicas.length > 0) {
      doc.fontSize(10).font('Helvetica-Bold');
      doc.text('Estruturas Tectônicas:', 40, doc.y + 10);
      doc.font('Helvetica').fontSize(9);
      data.tectonicas.slice(0, 5).forEach(item => {
        doc.text(`• ${item.nome} (${item.tipo})`, 50, doc.y);
      });
    }
  }

  /**
   * Add generic data section (fallback)
   */
  static _addGenericDataSection(doc, data) {
    if (data.data && Array.isArray(data.data)) {
      doc.fontSize(9).font('Helvetica');
      data.data.slice(0, 10).forEach(item => {
        const text = JSON.stringify(item).substring(0, 70);
        doc.text(`• ${text}...`, 50, doc.y);
      });
    } else {
      doc.fontSize(10).text('Sem dados estruturados', 40, doc.y);
    }
  }

  /**
   * Add table to PDF
   */
  static _addTable(doc, columns, rows) {
    const tableTop = doc.y;
    const rowHeight = 25;
    const pageHeight = doc.page.height - 50;

    // Draw header
    doc.fontSize(9).font('Helvetica-Bold').fillColor('#f0f4f8');
    doc.rect(40, tableTop, doc.page.width - 80, rowHeight).fill();

    doc.fillColor('black');
    let x = 40;
    columns.forEach(col => {
      doc.text(col.label, x + 5, tableTop + 7, { width: col.width - 10 });
      x += col.width;
    });

    // Draw rows
    let y = tableTop + rowHeight;
    rows.slice(0, 10).forEach((row, idx) => {
      if (y > pageHeight) {
        doc.addPage();
        y = 40;
      }

      doc.fontSize(9).font('Helvetica').fillColor(idx % 2 === 0 ? '#ffffff' : '#f9f9f9');
      doc.rect(40, y, doc.page.width - 80, rowHeight).fill();

      doc.fillColor('black');
      x = 40;
      row.forEach((cell, cidx) => {
        doc.text(String(cell).substring(0, 30), x + 5, y + 7, {
          width: columns[cidx].width - 10
        });
        x += columns[cidx].width;
      });

      y += rowHeight;
    });

    doc.y = y + 10;
  }

  /**
   * Add legal disclaimer
   */
  static _addLegalDisclaimer(doc) {
    doc.addPage();

    doc.fontSize(12).font('Helvetica-Bold').fillColor('#1a3a52');
    doc.text('Aviso Legal e Disclaimer', 40, 40);

    doc.moveTo(40, doc.y + 5).lineTo(doc.page.width - 40, doc.y + 5).stroke();
    doc.y += 25;

    doc.fontSize(9).font('Helvetica').fillColor('black');

    const disclaimer = [
      '1. AUTORIDADE LIMITADA',
      'Este relatório foi gerado automaticamente pela plataforma AdGain com base em dados geoespaciais públicos. As análises apresentadas são informativas e não substituem estudos técnicos específicos ou parecerestáticos profissionais.',
      '',
      '2. ACURÁCIA DOS DADOS',
      'Embora os dados utilizados sejam de fontes reconhecidas (INCRA, IBGE, INPE, etc.), não há garantia de acurácia completa. Recomenda-se sempre validar os resultados com fontes originais e consultores especializados.',
      '',
      '3. USO RESPONSÁVEL',
      'Este documento é fornecido para fins informativos e de planejamento. Não deve ser utilizado como única base para decisões legais, regulatórias ou de investimento sem validação adicional.',
      '',
      '4. RESPONSABILIDADE',
      'Os desenvolvedores e mantenedores da plataforma AdGain não serão responsáveis por erros, omissões ou decisões tomadas baseadas neste relatório.',
      '',
      '5. DADOS PÚBLICOS',
      'As análises utilizam dados públicos. Consulte a legislação vigente sobre uso de dados geoespaciais no Brasil.',
    ];

    disclaimer.forEach(line => {
      if (line === '') {
        doc.y += 5;
      } else if (line.match(/^\d+\./)) {
        doc.fontSize(10).font('Helvetica-Bold');
        doc.text(line, 40, doc.y);
      } else {
        doc.fontSize(9).font('Helvetica');
        doc.text(line, 40, doc.y, { width: doc.page.width - 80 });
      }
    });

    // Footer
    doc.fontSize(8).fillColor('#999');
    doc.text(
      `Documento gerado em ${new Date().toLocaleString('pt-BR')} pela plataforma AdGain v1.0`,
      40,
      doc.page.height - 40,
      { align: 'center' }
    );
  }

  /**
   * Helper to get area data
   */
  static _getAreaData(analyses) {
    // Try to find total area from any source
    if (analyses['9.3_solo'] && analyses['9.3_solo'].data) {
      // Calculate from soil data percentages (would need total area)
      return 'N/A';
    }
    return 'N/A';
  }
}

module.exports = ReportService;

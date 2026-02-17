/* ============================================
   Purple Wave Cascading Geo Filters
   Region → District → Territory
   ============================================ */

class PWGeoFilters {
  constructor({ data, regionId, districtId, territoryId, onChange }) {
    this.data = data;
    this.regionEl = document.getElementById(regionId);
    this.districtEl = document.getElementById(districtId);
    this.territoryEl = document.getElementById(territoryId);
    this.onChange = onChange;

    this._populate();
    this._bind();
  }

  _populate() {
    const regions = [...new Set(this.data.map(r => r.region))].sort((a, b) => a - b);
    this.regionEl.innerHTML = '<option value="all">All Regions</option>' +
      regions.map(r => `<option value="${r}">Region ${r}</option>`).join('');
    this.districtEl.innerHTML = '<option value="all">All Districts</option>';
    this.districtEl.disabled = true;
    this.territoryEl.innerHTML = '<option value="all">All Territories</option>';
    this.territoryEl.disabled = true;
  }

  _bind() {
    this.regionEl.addEventListener('change', () => {
      this._updateDistricts();
      this.territoryEl.value = 'all';
      this.territoryEl.disabled = true;
      this.territoryEl.innerHTML = '<option value="all">All Territories</option>';
      this.onChange(this.values());
    });

    this.districtEl.addEventListener('change', () => {
      this._updateTerritories();
      this.onChange(this.values());
    });

    this.territoryEl.addEventListener('change', () => {
      this.onChange(this.values());
    });
  }

  _updateDistricts() {
    const region = this.regionEl.value;
    if (region === 'all') {
      this.districtEl.innerHTML = '<option value="all">All Districts</option>';
      this.districtEl.disabled = true;
      return;
    }
    this.districtEl.disabled = false;
    const districts = [...new Set(
      this.data.filter(r => r.region == region).map(r => r.district)
    )].sort((a, b) => a - b);
    this.districtEl.innerHTML = '<option value="all">All Districts</option>' +
      districts.map(d => `<option value="${d}">District ${d}</option>`).join('');
  }

  _updateTerritories() {
    const region = this.regionEl.value;
    const district = this.districtEl.value;
    if (district === 'all') {
      this.territoryEl.innerHTML = '<option value="all">All Territories</option>';
      this.territoryEl.disabled = true;
      return;
    }
    this.territoryEl.disabled = false;
    const territories = [...new Set(
      this.data.filter(r => r.region == region && r.district == district).map(r => r.territory)
    )].sort((a, b) => a - b);
    this.territoryEl.innerHTML = '<option value="all">All Territories</option>' +
      territories.map(t => `<option value="${t}">Territory ${t}</option>`).join('');
  }

  values() {
    return {
      region: this.regionEl.value,
      district: this.districtEl.value,
      territory: this.territoryEl.value
    };
  }

  reset() {
    this.regionEl.value = 'all';
    this.districtEl.value = 'all';
    this.territoryEl.value = 'all';
    this.districtEl.disabled = true;
    this.territoryEl.disabled = true;
    this.onChange(this.values());
  }

  // Determine the current drill level and appropriate groupBy key
  drillLevel() {
    const v = this.values();
    if (v.territory !== 'all') return { level: 'territory', groupKey: 'territory', label: 'Territory' };
    if (v.district !== 'all') return { level: 'district', groupKey: 'territory', label: 'Territory' };
    if (v.region !== 'all') return { level: 'region', groupKey: 'district', label: 'District' };
    return { level: 'company', groupKey: 'region', label: 'Region' };
  }

  // Filter data based on current selections
  filterData(data) {
    const v = this.values();
    let filtered = data;
    if (v.region !== 'all') filtered = filtered.filter(r => r.region == v.region);
    if (v.district !== 'all') filtered = filtered.filter(r => r.district == v.district);
    if (v.territory !== 'all') filtered = filtered.filter(r => r.territory == v.territory);
    return filtered;
  }

  // Scope label text
  scopeText() {
    const v = this.values();
    if (v.region === 'all') return null;
    let label = `Region ${v.region}`;
    if (v.district !== 'all') label += ` → District ${v.district}`;
    if (v.territory !== 'all') label += ` → Territory ${v.territory}`;
    return label;
  }
}

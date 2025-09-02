# Un-hide and use this explore, or copy the joins into another explore, to get all the fully nested relationships from this view
explore: d_item_v3 {
  hidden: yes
    join: d_item_v3__accreditation {
      view_label: "D Item V3: Accreditation"
      sql: LEFT JOIN UNNEST(${d_item_v3.accreditation}) as d_item_v3__accreditation ;;
      relationship: one_to_many
    }
    join: d_item_v3__country_of_origin {
      view_label: "D Item V3: Country Of Origin"
      sql: LEFT JOIN UNNEST(${d_item_v3.country_of_origin}) as d_item_v3__country_of_origin ;;
      relationship: one_to_many
    }
    join: d_item_v3__central_department {
      view_label: "D Item V3: Central Department"
      sql: LEFT JOIN UNNEST(${d_item_v3.central_department}) as d_item_v3__central_department ;;
      relationship: one_to_many
    }
    join: d_item_v3__load_carrier_deposit {
      view_label: "D Item V3: Load Carrier Deposit"
      sql: LEFT JOIN UNNEST(${d_item_v3.load_carrier_deposit}) as d_item_v3__load_carrier_deposit ;;
      relationship: one_to_many
    }
    join: d_item_v3__ica_swedish_accreditation {
      view_label: "D Item V3: Ica Swedish Accreditation"
      sql: LEFT JOIN UNNEST(${d_item_v3.ica_swedish_accreditation}) as d_item_v3__ica_swedish_accreditation ;;
      relationship: one_to_many
    }
    join: d_item_v3__ica_ethical_accreditation {
      view_label: "D Item V3: Ica Ethical Accreditation"
      sql: LEFT JOIN UNNEST(${d_item_v3.ica_ethical_accreditation}) as d_item_v3__ica_ethical_accreditation ;;
      relationship: one_to_many
    }
    join: d_item_v3__ica_ecological_accreditation {
      view_label: "D Item V3: Ica Ecological Accreditation"
      sql: LEFT JOIN UNNEST(${d_item_v3.ica_ecological_accreditation}) as d_item_v3__ica_ecological_accreditation ;;
      relationship: one_to_many
    }
    join: d_item_v3__item_information_claim_detail {
      view_label: "D Item V3: Item Information Claim Detail"
      sql: LEFT JOIN UNNEST(${d_item_v3.item_information_claim_detail}) as d_item_v3__item_information_claim_detail ;;
      relationship: one_to_many
    }
    join: d_item_v3__ica_environmental_accreditation {
      view_label: "D Item V3: Ica Environmental Accreditation"
      sql: LEFT JOIN UNNEST(${d_item_v3.ica_environmental_accreditation}) as d_item_v3__ica_environmental_accreditation ;;
      relationship: one_to_many
    }
    join: d_item_v3__ica_non_ecological_accreditation {
      view_label: "D Item V3: Ica Non Ecological Accreditation"
      sql: LEFT JOIN UNNEST(${d_item_v3.ica_non_ecological_accreditation}) as d_item_v3__ica_non_ecological_accreditation ;;
      relationship: one_to_many
    }
    join: d_item_v3__packaging_information__packaging_material_composition {
      view_label: "D Item V3: Packaging Information Packaging Material Composition"
      sql: LEFT JOIN UNNEST(${d_item_v3.packaging_information__packaging_material_composition}) as d_item_v3__packaging_information__packaging_material_composition ;;
      relationship: one_to_many
    }
    join: d_item_v3__packaging_information__packaging_material_composition__packaging_material_composition_quantity {
      view_label: "D Item V3: Packaging Information Packaging Material Composition Packaging Material Composition Quantity"
      sql: LEFT JOIN UNNEST(${d_item_v3__packaging_information__packaging_material_composition.packaging_material_composition_quantity}) as d_item_v3__packaging_information__packaging_material_composition__packaging_material_composition_quantity ;;
      relationship: one_to_many
    }
}
view: d_item_v3 {
  sql_table_name: `ac16-p-conlaybi-prd-4257.item_versioned.d_item_v3` ;;

  dimension: accreditation {
    hidden: yes
    sql: ${TABLE}.accreditation ;;
  }
  dimension: aggregated_base_item_quantity {
    type: number
    description: "total quantity of base items in this GTIN , based on packstucture information"
    sql: ${TABLE}.aggregated_base_item_quantity ;;
  }
  dimension: aggregated_deposit_amount {
    type: number
    description: "total deposit amount including VAT (based aggregated_base_item_quantity and specified amount for each base item)"
    sql: ${TABLE}.aggregated_deposit_amount ;;
  }
  dimension: alcohol_percentage_by_volume {
    type: number
    description: "(T2208) Percentage of alcohol contained in the base unit trade item"
    sql: ${TABLE}.alcohol_percentage_by_volume ;;
  }
  dimension: assortment_attributes__ecological {
    type: string
    description: "Indicates if item has any markings that is considerad as ecological/organic; Ekologisk märkning / Saknar Ekologisk märkning"
    sql: ${TABLE}.assortment_attributes.ecological ;;
    group_label: "Assortment Attributes"
    group_item_label: "Ecological"
  }
  dimension: assortment_attributes__environmental {
    type: string
    description: "Indicates if item has any markings that is considerad as environmentally good; Miljömärkt / Saknar miljömärkning"
    sql: ${TABLE}.assortment_attributes.environmental ;;
    group_label: "Assortment Attributes"
    group_item_label: "Environmental"
  }
  dimension: assortment_attributes__environmental_non_ecological {
    type: string
    description: "Indicates if item has any markings that is considerad as environmentally good; Miljömärkt / Saknar miljömärkning"
    sql: ${TABLE}.assortment_attributes.environmental_non_ecological ;;
    group_label: "Assortment Attributes"
    group_item_label: "Environmental Non Ecological"
  }
  dimension: assortment_attributes__ethical {
    type: string
    description: "Indicates if item has any markings that is considerad as Ethical; Etisk märkning / Saknar etisk märkning"
    sql: ${TABLE}.assortment_attributes.ethical ;;
    group_label: "Assortment Attributes"
    group_item_label: "Ethical"
  }
  dimension: assortment_attributes__gdpr_sensitive__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.gdpr_sensitive.code_description ;;
    group_label: "Assortment Attributes Gdpr Sensitive"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__gdpr_sensitive__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.gdpr_sensitive.code_name ;;
    group_label: "Assortment Attributes Gdpr Sensitive"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__gdpr_sensitive__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.gdpr_sensitive.code_value ;;
    group_label: "Assortment Attributes Gdpr Sensitive"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__health {
    type: string
    description: "Indicates if item is \"healthy\" or not; Yes/No"
    sql: ${TABLE}.assortment_attributes.health ;;
    group_label: "Assortment Attributes"
    group_item_label: "Health"
  }
  dimension: assortment_attributes__ica_swedish__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.ica_swedish.code_description ;;
    group_label: "Assortment Attributes Ica Swedish"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__ica_swedish__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.ica_swedish.code_name ;;
    group_label: "Assortment Attributes Ica Swedish"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__ica_swedish__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.ica_swedish.code_value ;;
    group_label: "Assortment Attributes Ica Swedish"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__multicultural__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.multicultural.code_description ;;
    group_label: "Assortment Attributes Multicultural"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__multicultural__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.multicultural.code_name ;;
    group_label: "Assortment Attributes Multicultural"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__multicultural__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.multicultural.code_value ;;
    group_label: "Assortment Attributes Multicultural"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__pack_variant__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.pack_variant.code_description ;;
    group_label: "Assortment Attributes Pack Variant"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__pack_variant__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.pack_variant.code_name ;;
    group_label: "Assortment Attributes Pack Variant"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__pack_variant__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.pack_variant.code_value ;;
    group_label: "Assortment Attributes Pack Variant"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__packing_size__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.packing_size.code_description ;;
    group_label: "Assortment Attributes Packing Size"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__packing_size__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.packing_size.code_name ;;
    group_label: "Assortment Attributes Packing Size"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__packing_size__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.packing_size.code_value ;;
    group_label: "Assortment Attributes Packing Size"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__plantbased__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.plantbased.code_description ;;
    group_label: "Assortment Attributes Plantbased"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__plantbased__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.plantbased.code_name ;;
    group_label: "Assortment Attributes Plantbased"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__plantbased__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.plantbased.code_value ;;
    group_label: "Assortment Attributes Plantbased"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__price_range__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.price_range.code_description ;;
    group_label: "Assortment Attributes Price Range"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__price_range__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.price_range.code_name ;;
    group_label: "Assortment Attributes Price Range"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__price_range__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.price_range.code_value ;;
    group_label: "Assortment Attributes Price Range"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__quality__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.quality.code_description ;;
    group_label: "Assortment Attributes Quality"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__quality__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.quality.code_name ;;
    group_label: "Assortment Attributes Quality"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__quality__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.quality.code_value ;;
    group_label: "Assortment Attributes Quality"
    group_item_label: "Code Value"
  }
  dimension: assortment_attributes__sustainable {
    type: string
    description: "Indicates if item has any markings that is considerad as Sustainable; Hållbar / Ej hållbar"
    sql: ${TABLE}.assortment_attributes.sustainable ;;
    group_label: "Assortment Attributes"
    group_item_label: "Sustainable"
  }
  dimension: assortment_attributes__swedish__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.swedish.code_description ;;
    group_label: "Assortment Attributes Swedish"
    group_item_label: "Code Description"
  }
  dimension: assortment_attributes__swedish__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.swedish.code_name ;;
    group_label: "Assortment Attributes Swedish"
    group_item_label: "Code Name"
  }
  dimension: assortment_attributes__swedish__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.swedish.code_value ;;
    group_label: "Assortment Attributes Swedish"
    group_item_label: "Code Value"
  }
  dimension: bica_calculated_fields__bica_improved_ecological_markup {
    type: string
    description: "Using ecological_markup and additional information from item_description to retrive if it's an Eco / Krav product"
    sql: ${TABLE}.bica_calculated_fields.bica_improved_ecological_markup ;;
    group_label: "Bica Calculated Fields"
    group_item_label: "Bica Improved Ecological Markup"
  }
  dimension: bica_calculated_fields__bica_improved_weight_volume {
    type: number
    description: "Calculated field - Parsed weight or volume from field descriptive_size if net_content_in_gram or net_content_in_miligram is null"
    sql: ${TABLE}.bica_calculated_fields.bica_improved_weight_volume ;;
    group_label: "Bica Calculated Fields"
    group_item_label: "Bica Improved Weight Volume"
  }
  dimension: bica_calculated_fields__bica_improved_weight_volume_uom {
    type: string
    description: "Calculated field - Unit of measure value associated to bica_improved_weight_volume"
    sql: ${TABLE}.bica_calculated_fields.bica_improved_weight_volume_uom ;;
    group_label: "Bica Calculated Fields"
    group_item_label: "Bica Improved Weight Volume Uom"
  }
  dimension: brand__code_description {
    type: string
    sql: ${TABLE}.brand.code_description ;;
    group_label: "Brand"
    group_item_label: "Code Description"
  }
  dimension: brand__code_name {
    type: string
    sql: ${TABLE}.brand.code_name ;;
    group_label: "Brand"
    group_item_label: "Code Name"
  }
  dimension: brand__code_value {
    type: string
    sql: ${TABLE}.brand.code_value ;;
    group_label: "Brand"
    group_item_label: "Code Value"
  }
  dimension: catchweight_type_cd {
    type: string
    description: "(Record) Possibillity to flag items as solid weight even if the GS1 information says it's not. It can both be items with variable weight or not. 'Solid' or 'Exact"
    sql: ${TABLE}.catchweight_type_cd ;;
  }
  dimension: category_description {
    type: string
    description: "Merchandise hierarchy node category description; concatenation of id and name; e.g 7101 - Asiatiska köket"
    sql: ${TABLE}.category_description ;;
  }
  dimension: category_id {
    type: string
    description: "Merchandise hierarchy node category id; e.g 7101"
    sql: ${TABLE}.category_id ;;
  }
  dimension: category_name {
    type: string
    description: "Merchandise hierarchy node category name; e.g Asiatiska köket"
    sql: ${TABLE}.category_name ;;
  }
  dimension: category_specific_attributes__colour__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.colour.code_description ;;
    group_label: "Category Specific Attributes Colour"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__colour__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.colour.code_name ;;
    group_label: "Category Specific Attributes Colour"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__colour__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.colour.code_value ;;
    group_label: "Category Specific Attributes Colour"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__consumer_group__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.consumer_group.code_description ;;
    group_label: "Category Specific Attributes Consumer Group"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__consumer_group__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.consumer_group.code_name ;;
    group_label: "Category Specific Attributes Consumer Group"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__consumer_group__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.consumer_group.code_value ;;
    group_label: "Category Specific Attributes Consumer Group"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__execution1__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution1.code_description ;;
    group_label: "Category Specific Attributes Execution1"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__execution1__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution1.code_name ;;
    group_label: "Category Specific Attributes Execution1"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__execution1__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution1.code_value ;;
    group_label: "Category Specific Attributes Execution1"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__execution2__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution2.code_description ;;
    group_label: "Category Specific Attributes Execution2"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__execution2__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution2.code_name ;;
    group_label: "Category Specific Attributes Execution2"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__execution2__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution2.code_value ;;
    group_label: "Category Specific Attributes Execution2"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__execution3__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution3.code_description ;;
    group_label: "Category Specific Attributes Execution3"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__execution3__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution3.code_name ;;
    group_label: "Category Specific Attributes Execution3"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__execution3__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution3.code_value ;;
    group_label: "Category Specific Attributes Execution3"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__execution4__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution4.code_description ;;
    group_label: "Category Specific Attributes Execution4"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__execution4__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution4.code_name ;;
    group_label: "Category Specific Attributes Execution4"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__execution4__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution4.code_value ;;
    group_label: "Category Specific Attributes Execution4"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__flavour__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.flavour.code_description ;;
    group_label: "Category Specific Attributes Flavour"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__flavour__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.flavour.code_name ;;
    group_label: "Category Specific Attributes Flavour"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__flavour__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.flavour.code_value ;;
    group_label: "Category Specific Attributes Flavour"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__origin__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.origin.code_description ;;
    group_label: "Category Specific Attributes Origin"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__origin__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.origin.code_name ;;
    group_label: "Category Specific Attributes Origin"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__origin__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.origin.code_value ;;
    group_label: "Category Specific Attributes Origin"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__preparation__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.preparation.code_description ;;
    group_label: "Category Specific Attributes Preparation"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__preparation__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.preparation.code_name ;;
    group_label: "Category Specific Attributes Preparation"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__preparation__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.preparation.code_value ;;
    group_label: "Category Specific Attributes Preparation"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__product_group__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.product_group.code_description ;;
    group_label: "Category Specific Attributes Product Group"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__product_group__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.product_group.code_name ;;
    group_label: "Category Specific Attributes Product Group"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__product_group__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.product_group.code_value ;;
    group_label: "Category Specific Attributes Product Group"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__raw_material__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.raw_material.code_description ;;
    group_label: "Category Specific Attributes Raw Material"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__raw_material__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.raw_material.code_name ;;
    group_label: "Category Specific Attributes Raw Material"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__raw_material__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.raw_material.code_value ;;
    group_label: "Category Specific Attributes Raw Material"
    group_item_label: "Code Value"
  }
  dimension: category_specific_attributes__specific_content__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.specific_content.code_description ;;
    group_label: "Category Specific Attributes Specific Content"
    group_item_label: "Code Description"
  }
  dimension: category_specific_attributes__specific_content__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.specific_content.code_name ;;
    group_label: "Category Specific Attributes Specific Content"
    group_item_label: "Code Name"
  }
  dimension: category_specific_attributes__specific_content__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.specific_content.code_value ;;
    group_label: "Category Specific Attributes Specific Content"
    group_item_label: "Code Value"
  }
  dimension: central_department {
    hidden: yes
    sql: ${TABLE}.central_department ;;
  }
  dimension: consumer_item_reference__consumer_item_id {
    type: string
    description: "Identifier for the Consumer item, equivalent to EMS Store Item number"
    sql: ${TABLE}.consumer_item_reference.consumer_item_id ;;
    group_label: "Consumer Item Reference"
    group_item_label: "Consumer Item ID"
  }
  dimension: consumer_item_reference__plu_number {
    type: string
    description: "PriceLookUp; an ICA internal number that the cashier in store can use for alternative way of sales"
    sql: ${TABLE}.consumer_item_reference.plu_number ;;
    group_label: "Consumer Item Reference"
    group_item_label: "Plu Number"
  }
  dimension: core_input_reason__code_description {
    type: string
    sql: ${TABLE}.core_input_reason.code_description ;;
    group_label: "Core Input Reason"
    group_item_label: "Code Description"
  }
  dimension: core_input_reason__code_name {
    type: string
    sql: ${TABLE}.core_input_reason.code_name ;;
    group_label: "Core Input Reason"
    group_item_label: "Code Name"
  }
  dimension: core_input_reason__code_value {
    type: string
    sql: ${TABLE}.core_input_reason.code_value ;;
    group_label: "Core Input Reason"
    group_item_label: "Code Value"
  }
  dimension: country_of_origin {
    hidden: yes
    sql: ${TABLE}.country_of_origin ;;
  }
  dimension: css_main_category_group_description {
    type: string
    description: "CSS (Central sortimentstruktur) main category group"
    sql: ${TABLE}.css_main_category_group_description ;;
  }
  dimension: css_main_category_group_id {
    type: string
    description: "CSS (Central sortimentstruktur) main category group"
    sql: ${TABLE}.css_main_category_group_id ;;
  }
  dimension: css_main_category_group_name {
    type: string
    description: "CSS (Central sortimentstruktur) main category group"
    sql: ${TABLE}.css_main_category_group_name ;;
  }
  dimension: d_item_key {
    type: number
    description: "Technical key for d_item, derived from GTIN"
    sql: ${TABLE}.d_item_key ;;
  }
  dimension: descriptive_size {
    type: string
    description: "Descriptive size information."
    sql: ${TABLE}.descriptive_size ;;
  }
  dimension: division_description {
    type: string
    description: "Merchandise hierarchy node category description; concatenation of id and name; e.g 7101 - Asiatiska köket"
    sql: ${TABLE}.division_description ;;
  }
  dimension: division_id {
    type: string
    description: "Merchandise hierarchy node Division id; e.g 01"
    sql: ${TABLE}.division_id ;;
  }
  dimension: division_name {
    type: string
    description: "Merchandise hierarchy node category name; e.g Asiatiska köket"
    sql: ${TABLE}.division_name ;;
  }
  dimension: ecr_category__code_description {
    type: string
    sql: ${TABLE}.ecr_category.code_description ;;
    group_label: "Ecr Category"
    group_item_label: "Code Description"
  }
  dimension: ecr_category__code_name {
    type: string
    sql: ${TABLE}.ecr_category.code_name ;;
    group_label: "Ecr Category"
    group_item_label: "Code Name"
  }
  dimension: ecr_category__code_value {
    type: string
    sql: ${TABLE}.ecr_category.code_value ;;
    group_label: "Ecr Category"
    group_item_label: "Code Value"
  }
  dimension_group: ecr_revision {
    type: time
    description: "Launch date (FPH/ECR Calander) chosen by the supplier in the product portal. Category Manager can change date"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.ecr_revision_date ;;
  }
  dimension: functional_name {
    type: string
    description: "Item friendly name"
    sql: ${TABLE}.functional_name ;;
  }
  dimension: global_trade_item_number {
    type: string
    description: "(T0154) GTIN (Global Trade Item Number, GS1-artikelnummer)"
    sql: ${TABLE}.global_trade_item_number ;;
  }
  dimension: gpc_category_code {
    type: string
    description: "(T0280) Code specifying a product category according to the GS1 Global Product Classification (GPC) standard."
    sql: ${TABLE}.gpc_category_code ;;
  }
  dimension: gpc_category_definition {
    type: string
    description: "A GS1 supplied definition associated with the specified Global Product Classification (GPC) category code."
    sql: ${TABLE}.gpc_category_definition ;;
  }
  dimension: gpc_category_name {
    type: string
    description: "Name associated with the specified Global Product Classification (GPC) category code."
    sql: ${TABLE}.gpc_category_name ;;
  }
  dimension: ica_ecological_accreditation {
    hidden: yes
    sql: ${TABLE}.ica_ecological_accreditation ;;
  }
  dimension: ica_environmental_accreditation {
    hidden: yes
    sql: ${TABLE}.ica_environmental_accreditation ;;
  }
  dimension: ica_ethical_accreditation {
    hidden: yes
    sql: ${TABLE}.ica_ethical_accreditation ;;
  }
  dimension: ica_non_ecological_accreditation {
    hidden: yes
    sql: ${TABLE}.ica_non_ecological_accreditation ;;
  }
  dimension: ica_swedish_accreditation {
    hidden: yes
    sql: ${TABLE}.ica_swedish_accreditation ;;
  }
  dimension: information_providing_supplier {
    type: string
    description: "(Record)  Supplier that has been associated with the Item NOTE! It is the Information provider that will be used for the item information in FPH"
    sql: ${TABLE}.information_providing_supplier ;;
  }
  dimension: is_base_unit {
    type: yesno
    description: "(T4012) An indicator identifying the trade item as the base unit level of the trade item hierarchy."
    sql: ${TABLE}.is_base_unit ;;
  }
  dimension: is_bonus_item {
    type: yesno
    description: "If the item will give ICA bonus to end customer or not"
    sql: ${TABLE}.is_bonus_item ;;
  }
  dimension: is_catchweight_item {
    type: yesno
    description: "This attribute determine if ICA sees an item as a catch weight item."
    sql: ${TABLE}.is_catchweight_item ;;
  }
  dimension: is_consumer_unit {
    type: yesno
    description: "(T4037) Identifies whether the trade item to be taken possession of ,or to be consumed or used by an end user or both, as determined by the manufacturer. The end user could be, but is not limited to, a consumer as in items sold at retail, or a patient/clinician/technician in a healthcare setting, or an operator for foodservice such as restaurants, airlines, cafeterias, etc."
    sql: ${TABLE}.is_consumer_unit ;;
  }
  dimension: is_corporate_brand {
    type: yesno
    description: "Attribute indicating if the item is Coperate Brand based on rule (list of Coperate Brands)"
    sql: ${TABLE}.is_corporate_brand ;;
  }
  dimension: is_despatch_unit {
    type: yesno
    description: "(T4038) An indicator identifying that the information providerconsiders the trade item as a despatch (shipping) unit. Thismay be relationship dependent based on channel of tradeor other point to point agreement."
    sql: ${TABLE}.is_despatch_unit ;;
  }
  dimension: is_ica_external_sourcing {
    type: yesno
    description: "Attribute that indicates wether the specified item is a Central or External item. True = 'External' False = 'Central'"
    sql: ${TABLE}.is_ica_external_sourcing ;;
  }
  dimension: is_invoice_unit {
    type: yesno
    description: "(T4014) An indicator identifying that the information provider willinclude this trade item on their billing or invoice. This maybe relationship dependent based on channel of trade orother point to point agreement."
    sql: ${TABLE}.is_invoice_unit ;;
  }
  dimension: is_orderable_unit {
    type: yesno
    description: "(T0017) An indicator identifying that the information provider considers this trade item to be at a hierarchy level wherethey will accept orders from customers. This may bedifferent from what the information provider identifies as adespatch unit. This may be a relationship dependent basedon channel of trade or other point to point agreement"
    sql: ${TABLE}.is_orderable_unit ;;
  }
  dimension: is_private_label {
    type: yesno
    description: "Attribute indicating if the item is a Private Label, that is an ICA branded product (aka EMV)."
    sql: ${TABLE}.is_private_label ;;
  }
  dimension: is_scale_plu {
    type: yesno
    description: "If an item is a scale-PLU item"
    sql: ${TABLE}.is_scale_plu ;;
  }
  dimension: is_seasonal {
    type: yesno
    description: "Shows if an item is seasonal."
    sql: ${TABLE}.is_seasonal ;;
  }
  dimension: item_description {
    type: string
    description: "The ItemDescription consists of the combination of the GS1 attributes Brand Name, Item Name and Article Size"
    sql: ${TABLE}.item_description ;;
  }
  dimension: item_id {
    type: string
    description: "(T0154) GTIN (Global Trade Item Number, GS1-artikelnummer)"
    sql: ${TABLE}.item_id ;;
  }
  dimension: item_information_claim_detail {
    hidden: yes
    sql: ${TABLE}.item_information_claim_detail ;;
  }
  dimension: item_pack_type {
    type: string
    description: "The pack type of the item; Pallet, Case, \"Base Unit or Each\" or empty."
    sql: ${TABLE}.item_pack_type ;;
  }
  dimension: item_reporting_description {
    type: string
    description: "The item_reporting_description consists of either description, short_description or item_description from s_consumer_item_main or s_item_main"
    sql: ${TABLE}.item_reporting_description ;;
  }
  dimension: item_reporting_id {
    type: string
    description: "The item_reporting_id is for display purpose, consists of either global trade item number or item part  where item id represents store unique items (item_id contains |##|)"
    sql: ${TABLE}.item_reporting_id ;;
  }
  dimension: lifecycle__central_status {
    type: string
    description: "This is the current status of the item Possible values: Draft - This is the item status throughout the Proposed to Accepted process. Once the Proposed to Accepted attribute is set to accepted the item status can be updated to be 'New' New - The Item is now approved for assortment in ICA but some final enrichment is still needed. Active - The item can be set to 'Active' once all criteria is met Phase-Out - An item is set to 'Phase-Out' when the delist date field is populated. On-Hold - The on-hold status simply stops all sell/purchasing of the item. Inactive - Item is made 'Inactive' it has now been either delisted/discontinued or ICA want to remove item Obsolete - Once in this status the item can be purged. It will need to be in the 'Obsolete' status for 24 months prior purging."
    sql: ${TABLE}.lifecycle.central_status ;;
    group_label: "Lifecycle"
    group_item_label: "Central Status"
  }
  dimension_group: lifecycle__creation_datetime {
    type: time
    description: "Automatic time stamp when item is created"
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.lifecycle.creation_datetime ;;
  }
  dimension_group: lifecycle__ica_discontinue {
    type: time
    description: "When ICA decides to discontinue an item. If the attributes is empty (an ICA internal decision is made before supplier sends in information), the discontinue date shall be copied from GS1 attribute."
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle.ica_discontinue_date ;;
  }
  dimension: lifecycle__ica_discontinue_reason {
    type: string
    description: "Reason for discontinueing the Item, either ICA or supplier. IF ItemEBO/PackStructure/Item/TradeItem/TradeItemSynchronisationDates/DiscontinuedDateTime is null THEN ItemEBO/PackStructure/Item/ItemStatuses/ICADiscontinueReason = 'ICA' ELSE 'SUPPLIER'"
    sql: ${TABLE}.lifecycle.ica_discontinue_reason ;;
    group_label: "Lifecycle"
    group_item_label: "Ica Discontinue Reason"
  }
  dimension: lifecycle__introduction_status {
    type: string
    description: "Mapping to the ICA End to End Process Status is for information purposes only. Only those for ItemEBO/PackStructure/Item/ItemStatuses/Status = 'DRAFT' will be modelled as a secondary status in FPH as an attribute called 'Item Introduction Status'"
    sql: ${TABLE}.lifecycle.introduction_status ;;
    group_label: "Lifecycle"
    group_item_label: "Introduction Status"
  }
  dimension_group: lifecycle__novelty_end {
    type: time
    description: "Enddate when item is considered as new"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle.novelty_end_date ;;
  }
  dimension_group: lifecycle__novelty_start {
    type: time
    description: "Startdate when item is considered as new"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle.novelty_start_date ;;
  }
  dimension: lifecycle__novelty_type {
    type: string
    description: "Type of novelty; e-g- New , Changed"
    sql: ${TABLE}.lifecycle.novelty_type ;;
    group_label: "Lifecycle"
    group_item_label: "Novelty Type"
  }
  dimension_group: lifecycle__obsolete {
    type: time
    description: "The date when the item record goes into status Obsolete"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle.obsolete_date ;;
  }
  dimension: lifecycle__on_hold_reason {
    type: string
    description: "Reason for onhold status; eg. ICA Delist, Supplier conflict, Supplier out of stock, Seasonal hold"
    sql: ${TABLE}.lifecycle.on_hold_reason ;;
    group_label: "Lifecycle"
    group_item_label: "On Hold Reason"
  }
  dimension_group: lifecycle__on_hold_start {
    type: time
    description: "Start date, when item no longer is active in assortment."
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle.on_hold_start_date ;;
  }
  dimension_group: lifecycle__purge {
    type: time
    description: "The date when the item record is to be removed from the FPH db"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle.purge_date ;;
  }
  dimension_group: lifecycle__reactivation {
    type: time
    description: "The date when the item will be reactivated"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle.reactivation_date ;;
  }
  dimension: load_carrier_deposit {
    hidden: yes
    sql: ${TABLE}.load_carrier_deposit ;;
  }
  dimension: main_category_description {
    type: string
    description: "Merchandise hierarchy node category description; concatenation of id and name; e.g 7101 - Asiatiska köket"
    sql: ${TABLE}.main_category_description ;;
  }
  dimension: main_category_id {
    type: string
    description: "Merchandise hierarchy node main category id; e.g 101"
    sql: ${TABLE}.main_category_id ;;
  }
  dimension: main_category_name {
    type: string
    description: "Merchandise hierarchy node category name; e.g Asiatiska köket"
    sql: ${TABLE}.main_category_name ;;
  }
  dimension: md_audit_seq {
    type: string
    description: "Technical field for specific dbt run"
    sql: ${TABLE}.md_audit_seq ;;
  }
  dimension_group: md_insert_dttm {
    type: time
    description: "Technical field insert datettime"
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: datetime
    sql: ${TABLE}.md_insert_dttm ;;
  }
  dimension: md_row_hash {
    type: number
    description: "Technical field for comparison of attributes"
    sql: ${TABLE}.md_row_hash ;;
  }
  dimension: measurements__depth {
    type: number
    description: "(T4018) The depth of the unit load, as measured according to the GS1 Package Measurement Rules, including the shipping platform unless it is excluded according to the Pallet Type Code chosen."
    sql: ${TABLE}.measurements.depth ;;
    group_label: "Measurements"
    group_item_label: "Depth"
  }
  dimension: measurements__depth_unit_of_measure {
    type: string
    description: "(T3780) unit of measure value associated to depth value"
    sql: ${TABLE}.measurements.depth_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Depth Unit of Measure"
  }
  dimension: measurements__gross_weight_in_gram {
    type: number
    description: "(T4020) Used to identify the gross weight of the trade item. The gross weight includes all packaging materials of the trade item. At pallet level the trade item, grossWeight includes the weight of the pallet itself. For example, 200 GRM, value - total pounds, total grams, etc. Has to be associated with a valid UOM."
    sql: ${TABLE}.measurements.gross_weight_in_gram ;;
    group_label: "Measurements"
    group_item_label: "Gross Weight In Gram"
  }
  dimension: measurements__height {
    type: number
    description: "(T4019) The height of the unit load, as measured according to the GS1 Package Measurement Rules, including the shipping platform unless it is excluded according to the Pallet Type Code chosen."
    sql: ${TABLE}.measurements.height ;;
    group_label: "Measurements"
    group_item_label: "Height"
  }
  dimension: measurements__height_unit_of_measure {
    type: string
    description: "(T3780) unit of measure value associated to height value"
    sql: ${TABLE}.measurements.height_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Height Unit of Measure"
  }
  dimension: measurements__net_content_in_gram {
    type: number
    description: "(T0082) The amount of the trade item contained by a package, usually as claimed on the label. For example, Water 750ml - net content = 750 MLT ; 20 count pack of diapers, net content = 20 ea.. In case of multi-pack, indicates the net content of the total trade item. For fixed value trade items use the value claimed on the package, to avoid variable fill rate issue that arises with some trade item which are sold by volume or weight, and whose actual content may vary slightly from batch to batch. In case of variable quantity trade items, indicates the average quantity. Allows for the representation of the same value in different units of measure but not multiple values. Only values having UOM = GRAM"
    sql: ${TABLE}.measurements.net_content_in_gram ;;
    group_label: "Measurements"
    group_item_label: "Net Content In Gram"
  }
  dimension: measurements__net_content_in_kilogram {
    type: number
    description: "(T0082) The amount of the trade item contained by a package, usually as claimed on the label. For example, Water 750ml - net content = 750 MLT ; 20 count pack of diapers, net content = 20 ea.. In case of multi-pack, indicates the net content of the total trade item. For fixed value trade items use the value claimed on the package, to avoid variable fill rate issue that arises with some trade item which are sold by volume or weight, and whose actual content may vary slightly from batch to batch. In case of variable quantity trade items, indicates the average quantity. Allows for the representation of the same value in different units of measure but not multiple values.Only values having UOM = KILOGRAM"
    sql: ${TABLE}.measurements.net_content_in_kilogram ;;
    group_label: "Measurements"
    group_item_label: "Net Content In Kilogram"
  }
  dimension: measurements__net_content_in_litre {
    type: number
    description: "(T0082) The amount of the trade item contained by a package, usually as claimed on the label. For example, Water 750ml - net content = 750 MLT ; 20 count pack of diapers, net content = 20 ea.. In case of multi-pack, indicates the net content of the total trade item. For fixed value trade items use the value claimed on the package, to avoid variable fill rate issue that arises with some trade item which are sold by volume or weight, and whose actual content may vary slightly from batch to batch. In case of variable quantity trade items, indicates the average quantity. Allows for the representation of the same value in different units of measure but not multiple values."
    sql: ${TABLE}.measurements.net_content_in_litre ;;
    group_label: "Measurements"
    group_item_label: "Net Content In Litre"
  }
  dimension: measurements__net_content_in_millilitre {
    type: number
    description: "(T0082) The amount of the trade item contained by a package, usually as claimed on the label. For example, Water 750ml - net content = 750 MLT ; 20 count pack of diapers, net content = 20 ea.. In case of multi-pack, indicates the net content of the total trade item. For fixed value trade items use the value claimed on the package, to avoid variable fill rate issue that arises with some trade item which are sold by volume or weight, and whose actual content may vary slightly from batch to batch. In case of variable quantity trade items, indicates the average quantity. Allows for the representation of the same value in different units of measure but not multiple values. Only values having UOM = MILLIITRE"
    sql: ${TABLE}.measurements.net_content_in_millilitre ;;
    group_label: "Measurements"
    group_item_label: "Net Content In Millilitre"
  }
  dimension: measurements__net_content_in_millimeter {
    type: number
    description: "(T0082) The amount of the trade item contained by a package, usually as claimed on the label. For example, Water 750ml - net content = 750 MLT ; 20 count pack of diapers, net content = 20 ea.. In case of multi-pack, indicates the net content of the total trade item. For fixed value trade items use the value claimed on the package, to avoid variable fill rate issue that arises with some trade item which are sold by volume or weight, and whose actual content may vary slightly from batch to batch. In case of variable quantity trade items, indicates the average quantity. Allows for the representation of the same value in different units of measure but not multiple values. Only values having UOM = MILLIMETER"
    sql: ${TABLE}.measurements.net_content_in_millimeter ;;
    group_label: "Measurements"
    group_item_label: "Net Content In Millimeter"
  }
  dimension: measurements__net_content_others {
    type: number
    description: "(T0082) The amount of the trade item contained by a package, usually as claimed on the label. For example, Water 750ml - net content = 750 MLT ; 20 count pack of diapers, net content = 20 ea.. In case of multi-pack, indicates the net content of the total trade item. For fixed value trade items use the value claimed on the package, to avoid variable fill rate issue that arises with some trade item which are sold by volume or weight, and whose actual content may vary slightly from batch to batch. In case of variable quantity trade items, indicates the average quantity. Allows for the representation of the same value in different units of measure but not multiple values. Only values having UOM not matching any of the other"
    sql: ${TABLE}.measurements.net_content_others ;;
    group_label: "Measurements"
    group_item_label: "Net Content Others"
  }
  dimension: measurements__net_content_others_unit_of_measure {
    type: string
    description: "(T3780) unit of measure value associated to net content others value"
    sql: ${TABLE}.measurements.net_content_others_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Net Content Others Unit of Measure"
  }
  dimension: measurements__net_content_per_piece {
    type: number
    description: "(T0082) The amount of the trade item contained by a package, usually as claimed on the label. For example, Water 750ml - net content = 750 MLT ; 20 count pack of diapers, net content = 20 ea.. In case of multi-pack, indicates the net content of the total trade item. For fixed value trade items use the value claimed on the package, to avoid variable fill rate issue that arises with some trade item which are sold by volume or weight, and whose actual content may vary slightly from batch to batch. In case of variable quantity trade items, indicates the average quantity. Allows for the representation of the same value in different units of measure but not multiple values. Only values having UOM = PIECE"
    sql: ${TABLE}.measurements.net_content_per_piece ;;
    group_label: "Measurements"
    group_item_label: "Net Content per Piece"
  }
  dimension: measurements__width {
    type: number
    description: "(T4017) The width of the unit load, as measured according to the GS1 Package Measurement Rules, including the shipping platform unless it is excluded according to the Pallet Type Code chosen."
    sql: ${TABLE}.measurements.width ;;
    group_label: "Measurements"
    group_item_label: "Width"
  }
  dimension: measurements__width_unit_of_measure {
    type: string
    description: "(T3780) unit of measure value associated to width value"
    sql: ${TABLE}.measurements.width_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Width Unit of Measure"
  }
  dimension: net_weight {
    type: number
    description: "The net weight in GRAM of the trade item. Autocalculated from GS1 attributes; 'Gross Weight' - 'Packaging weight'."
    sql: ${TABLE}.net_weight ;;
  }
  dimension: packaging_information__packaging_material_composition {
    hidden: yes
    sql: ${TABLE}.packaging_information.packaging_material_composition ;;
    group_label: "Packaging Information"
    group_item_label: "Packaging Material Composition"
  }
  dimension: packaging_information__packaging_weight {
    type: number
    description: "Used to identify the measurement of the packaging weight of the trade item."
    sql: ${TABLE}.packaging_information.packaging_weight ;;
    group_label: "Packaging Information"
    group_item_label: "Packaging Weight"
  }
  dimension: packaging_information__packaging_weight_uom {
    type: string
    description: "The Unit Of Measure for attribute PackagingWeight"
    sql: ${TABLE}.packaging_information.packaging_weight_uom ;;
    group_label: "Packaging Information"
    group_item_label: "Packaging Weight Uom"
  }
  dimension: price_comparison {
    type: number
    description: "The quantity of the product at usage. Applicable for concentrated products and products where the comparison price is calculated based on a measurement other than netContent. This field is dependent on the population of priceComparisonContentType and is required when priceComparisonContentType is used. Allows for the representation of the same value in different units of measure but not multiple values."
    sql: ${TABLE}.price_comparison ;;
  }
  dimension: price_comparison_unit_of_measure {
    type: string
    description: "The Unit Of Measure for the PriceComparisonMeasurement attribute"
    sql: ${TABLE}.price_comparison_unit_of_measure ;;
  }
  dimension_group: primary_soi_supplier_reference__delivery_start {
    type: time
    description: "Delivery Start Date is when the SOI is deliverable to stores"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.primary_soi_supplier_reference.delivery_start_date ;;
  }
  dimension: primary_soi_supplier_reference__is_primary_consumer_item_for_soi {
    type: yesno
    description: "Primary supplier and item used for purchasing"
    sql: ${TABLE}.primary_soi_supplier_reference.is_primary_consumer_item_for_soi ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Is Primary Consumer Item for Soi"
  }
  dimension_group: primary_soi_supplier_reference__orderability_end {
    type: time
    description: "Orderability is when the SOI is orderable for stores."
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.primary_soi_supplier_reference.orderability_end_date ;;
  }
  dimension_group: primary_soi_supplier_reference__orderability_start {
    type: time
    description: "Orderability is when the SOI is orderable for stores."
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.primary_soi_supplier_reference.orderability_start_date ;;
  }
  dimension: primary_soi_supplier_reference__soi_description {
    type: string
    description: "Description of SOI"
    sql: ${TABLE}.primary_soi_supplier_reference.soi_description ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Soi Description"
  }
  dimension: primary_soi_supplier_reference__soi_status {
    type: string
    description: "A current status for a SOI"
    sql: ${TABLE}.primary_soi_supplier_reference.soi_status ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Soi Status"
  }
  dimension: primary_soi_supplier_reference__store_orderable_item_id {
    type: string
    description: "This number is a unique identifier and represents the ICA SOI number aka the MAS artikelnummer"
    sql: ${TABLE}.primary_soi_supplier_reference.store_orderable_item_id ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Store Orderable Item ID"
  }
  dimension: primary_soi_supplier_reference__supplier_id {
    type: string
    description: "Supplier number in the Fusion Cloud application"
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_id ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Supplier ID"
  }
  dimension: primary_soi_supplier_reference__supplier_organization_name {
    type: string
    description: "The name of the Supplier"
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_organization_name ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Supplier Organization Name"
  }
  dimension: primary_soi_supplier_reference__supplier_site_description {
    type: string
    description: "Description of supplier site"
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_site_description ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Supplier Site Description"
  }
  dimension: primary_soi_supplier_reference__supplier_site_id {
    type: string
    description: "This is the end-user facing, unique Supplier Site number in the Fusion Cloud application"
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_site_id ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Supplier Site ID"
  }
  dimension: primary_soi_supplier_reference__supplychain_supplier_id {
    type: string
    description: "supplier identification used in supplychain aka MAS-leverantör"
    sql: ${TABLE}.primary_soi_supplier_reference.supplychain_supplier_id ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Supplychain Supplier ID"
  }
  dimension: primary_soi_supplier_reference__supplychain_supplier_long_name {
    type: string
    description: "Long name of supplychain supplier"
    sql: ${TABLE}.primary_soi_supplier_reference.supplychain_supplier_long_name ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Supplychain Supplier Long Name"
  }
  dimension: primary_soi_supplier_reference__supplychain_supplier_short_name {
    type: string
    description: "Short name of supplychain supplier"
    sql: ${TABLE}.primary_soi_supplier_reference.supplychain_supplier_short_name ;;
    group_label: "Primary Soi Supplier Reference"
    group_item_label: "Supplychain Supplier Short Name"
  }
  dimension: returnable_asset_deposit_name {
    type: string
    description: "(T0148) Depositname e.g. Engångs Pet över 1000 ml"
    sql: ${TABLE}.returnable_asset_deposit_name ;;
  }
  dimension: returnable_asset_deposit_type {
    type: string
    description: "(T0148) Type of deposit item (Container,Crate,LoadCarrier)"
    sql: ${TABLE}.returnable_asset_deposit_type ;;
  }
  dimension: season__code_description {
    type: string
    sql: ${TABLE}.season.code_description ;;
    group_label: "Season"
    group_item_label: "Code Description"
  }
  dimension: season__code_name {
    type: string
    sql: ${TABLE}.season.code_name ;;
    group_label: "Season"
    group_item_label: "Code Name"
  }
  dimension: season__code_value {
    type: string
    sql: ${TABLE}.season.code_value ;;
    group_label: "Season"
    group_item_label: "Code Value"
  }
  dimension_group: season_end {
    type: time
    description: "The end date for the season"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.season_end_date ;;
  }
  dimension_group: season_start {
    type: time
    description: "The start date for the season"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.season_start_date ;;
  }
  dimension: segment_description {
    type: string
    description: "Merchandise hierarchy node category description; concatenation of id and name; e.g 7101 - Asiatiska köket"
    sql: ${TABLE}.segment_description ;;
  }
  dimension: segment_id {
    type: string
    description: "Merchandise hierarchy node sub category id; e.g 7101.5.3 (prefixed by category_id and subcategory_id)"
    sql: ${TABLE}.segment_id ;;
  }
  dimension: segment_name {
    type: string
    description: "Merchandise hierarchy node category name; e.g Asiatiska köket"
    sql: ${TABLE}.segment_name ;;
  }
  dimension: standard_unit_of_measure {
    type: string
    description: "Automatically default to EACH for all catch weight orderable trade Items (not part of GS1 attribute)."
    sql: ${TABLE}.standard_unit_of_measure ;;
  }
  dimension: sub_category_description {
    type: string
    description: "Merchandise hierarchy node category description; concatenation of id and name; e.g 7101 - Asiatiska köket"
    sql: ${TABLE}.sub_category_description ;;
  }
  dimension: sub_category_id {
    type: string
    description: "Merchandise hierarchy node sub category id; e.g 7101.5 (prefixed by category_id)"
    sql: ${TABLE}.sub_category_id ;;
  }
  dimension: sub_category_name {
    type: string
    description: "Merchandise hierarchy node category name; e.g Asiatiska köket"
    sql: ${TABLE}.sub_category_name ;;
  }
  dimension: supply_chain_orderable_status {
    type: string
    description: "Used to know if ICA will order on this item record's level. Also used to keep track of Add Item process (will be ticked when agreement is set in BasICA)."
    sql: ${TABLE}.supply_chain_orderable_status ;;
  }
  dimension: vat_percent {
    type: number
    description: "(T0195) The current tax or duty rate percentage applicable to the trade item."
    sql: ${TABLE}.vat_percent ;;
  }
  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
	category_name,
	sub_category_name,
	functional_name,
	main_category_name,
	css_main_category_group_name,
	gpc_category_name,
	segment_name,
	returnable_asset_deposit_name,
	division_name,
	brand__code_name,
	season__code_name,
	ecr_category__code_name,
	core_input_reason__code_name,
	assortment_attributes__swedish__code_name,
	assortment_attributes__quality__code_name,
	assortment_attributes__plantbased__code_name,
	assortment_attributes__price_range__code_name,
	assortment_attributes__ica_swedish__code_name,
	assortment_attributes__packing_size__code_name,
	assortment_attributes__pack_variant__code_name,
	category_specific_attributes__colour__code_name,
	category_specific_attributes__origin__code_name,
	assortment_attributes__multicultural__code_name,
	category_specific_attributes__flavour__code_name,
	assortment_attributes__gdpr_sensitive__code_name,
	category_specific_attributes__execution1__code_name,
	category_specific_attributes__execution2__code_name,
	category_specific_attributes__execution3__code_name,
	category_specific_attributes__execution4__code_name,
	category_specific_attributes__preparation__code_name,
	category_specific_attributes__raw_material__code_name,
	category_specific_attributes__product_group__code_name,
	category_specific_attributes__consumer_group__code_name,
	category_specific_attributes__specific_content__code_name,
	primary_soi_supplier_reference__supplier_organization_name,
	primary_soi_supplier_reference__supplychain_supplier_long_name,
	primary_soi_supplier_reference__supplychain_supplier_short_name
	]
  }

}

view: d_item_v3__accreditation {

  dimension: accreditation_code {
    type: string
    sql: ${TABLE}.accreditation_code ;;
  }
  dimension: accreditation_description {
    type: string
    sql: ${TABLE}.accreditation_description ;;
  }
  dimension: accreditation_name {
    type: string
    sql: ${TABLE}.accreditation_name ;;
  }
  dimension: d_item_v3__accreditation {
    type: string
    description: "(T3777) All item acceditations (GS1 CodeList PackagingMarkedLabelAccreditationCode)"
    hidden: yes
    sql: d_item_v3__accreditation ;;
  }
}

view: d_item_v3__country_of_origin {

  dimension: d_item_v3__country_of_origin {
    type: string
    description: "The country the item may have originated from, has been processed in. Etc."
    sql: d_item_v3__country_of_origin ;;
  }
}

view: d_item_v3__central_department {

  dimension: central_department_code {
    type: string
    description: "department (used for central analysis close to store , maintained by Store and Marketing sponsor area)"
    sql: ${TABLE}.central_department_code ;;
  }
  dimension: central_department_description {
    type: string
    description: "department (used for central analysis close to store , maintained by Store and Marketing sponsor area)"
    sql: ${TABLE}.central_department_description ;;
  }
  dimension: central_department_name {
    type: string
    description: "department (used for central analysis close to store , maintained by Store and Marketing sponsor area)"
    sql: ${TABLE}.central_department_name ;;
  }
  dimension: d_item_v3__central_department {
    type: string
    description: "Department (used for central analysis close to store, maintained by Store and Marketing sponsor area)"
    hidden: yes
    sql: d_item_v3__central_department ;;
  }
  dimension: profile_id {
    type: string
    description: "Store profile GLN thats connected to current central department"
    sql: ${TABLE}.profile_id ;;
  }
  dimension: profile_name {
    type: string
    description: "Store profile name thats connected to current central department"
    sql: ${TABLE}.profile_name ;;
  }
}

view: d_item_v3__load_carrier_deposit {

  dimension: base_item_quantity {
    type: number
    description: "quantity of base items in this GTIN , based on packstucture information"
    sql: ${TABLE}.base_item_quantity ;;
  }
  dimension: d_item_v3__load_carrier_deposit {
    type: string
    description: "(Record) returnable asset details"
    hidden: yes
    sql: d_item_v3__load_carrier_deposit ;;
  }
  dimension: deposit_amount {
    type: number
    description: "deposit amount (returnable_asset_contained_quantity*returnable_package_deposit_amount)"
    sql: ${TABLE}.deposit_amount ;;
  }
  dimension: returnable_asset_contained_quantity {
    type: number
    description: "(T4125) Number of deposit items per item"
    sql: ${TABLE}.returnable_asset_contained_quantity ;;
  }
  dimension: returnable_asset_deposit_name {
    type: string
    description: "(T0148) Depositname e.g. Engångs Pet över 1000 ml"
    sql: ${TABLE}.returnable_asset_deposit_name ;;
  }
  dimension: returnable_asset_deposit_type {
    type: string
    description: "(T0148) Type of deposit item (Container,Crate,LoadCarrier)"
    sql: ${TABLE}.returnable_asset_deposit_type ;;
  }
  dimension: returnable_package_deposit_amount {
    type: number
    description: "(T0148) Deposit value per deposit asset incluiding VAT"
    sql: ${TABLE}.returnable_package_deposit_amount ;;
  }
}

view: d_item_v3__ica_swedish_accreditation {

  dimension: d_item_v3__ica_swedish_accreditation {
    type: string
    description: "(T3777) Item accreditations considered as swedish by ICA, see detail on BICA wiki, subset of accredition-attribute"
    hidden: yes
    sql: d_item_v3__ica_swedish_accreditation ;;
  }
  dimension: ica_swedish_accreditation_code {
    type: string
    sql: ${TABLE}.ica_swedish_accreditation_code ;;
  }
  dimension: ica_swedish_accreditation_description {
    type: string
    sql: ${TABLE}.ica_swedish_accreditation_description ;;
  }
  dimension: ica_swedish_accreditation_name {
    type: string
    sql: ${TABLE}.ica_swedish_accreditation_name ;;
  }
}

view: d_item_v3__ica_ethical_accreditation {

  dimension: d_item_v3__ica_ethical_accreditation {
    type: string
    description: "(T3777) Item accreditations considered as ethical by ICA, see detail on BICA wiki, subset of accredition-attribute"
    hidden: yes
    sql: d_item_v3__ica_ethical_accreditation ;;
  }
  dimension: ica_ethical_accreditation_code {
    type: string
    sql: ${TABLE}.ica_ethical_accreditation_code ;;
  }
  dimension: ica_ethical_accreditation_description {
    type: string
    sql: ${TABLE}.ica_ethical_accreditation_description ;;
  }
  dimension: ica_ethical_accreditation_name {
    type: string
    sql: ${TABLE}.ica_ethical_accreditation_name ;;
  }
}

view: d_item_v3__ica_ecological_accreditation {

  dimension: d_item_v3__ica_ecological_accreditation {
    type: string
    description: "(T3777) Item accreditations considered as environmental and ecological by ICA, see detail on BICA wiki, subset of accredition-attribute"
    hidden: yes
    sql: d_item_v3__ica_ecological_accreditation ;;
  }
  dimension: ica_ecological_accreditation_code {
    type: string
    sql: ${TABLE}.ica_ecological_accreditation_code ;;
  }
  dimension: ica_ecological_accreditation_description {
    type: string
    sql: ${TABLE}.ica_ecological_accreditation_description ;;
  }
  dimension: ica_ecological_accreditation_name {
    type: string
    sql: ${TABLE}.ica_ecological_accreditation_name ;;
  }
}

view: d_item_v3__item_information_claim_detail {

  dimension: claim_element__claim_element_code_description {
    type: string
    sql: ${TABLE}.claim_element.claim_element_code_description ;;
    group_label: "Claim Element"
    group_item_label: "Claim Element Code Description"
  }
  dimension: claim_element__claim_element_code_name {
    type: string
    sql: ${TABLE}.claim_element.claim_element_code_name ;;
    group_label: "Claim Element"
    group_item_label: "Claim Element Code Name"
  }
  dimension: claim_element__claim_element_code_value {
    type: string
    sql: ${TABLE}.claim_element.claim_element_code_value ;;
    group_label: "Claim Element"
    group_item_label: "Claim Element Code Value"
  }
  dimension: claim_type__claim_type_code_description {
    type: string
    sql: ${TABLE}.claim_type.claim_type_code_description ;;
    group_label: "Claim Type"
    group_item_label: "Claim Type Code Description"
  }
  dimension: claim_type__claim_type_code_name {
    type: string
    sql: ${TABLE}.claim_type.claim_type_code_name ;;
    group_label: "Claim Type"
    group_item_label: "Claim Type Code Name"
  }
  dimension: claim_type__claim_type_code_value {
    type: string
    sql: ${TABLE}.claim_type.claim_type_code_value ;;
    group_label: "Claim Type"
    group_item_label: "Claim Type Code Value"
  }
  dimension: d_item_v3__item_information_claim_detail {
    type: string
    description: "(T4357, T4358, T4359) Item information claim details"
    hidden: yes
    sql: d_item_v3__item_information_claim_detail ;;
  }
  dimension: is_item_information_claim_marked_on_package {
    type: yesno
    description: "(T4357) Item information claim details is marked on packaage (true/false)"
    sql: ${TABLE}.is_item_information_claim_marked_on_package ;;
  }
  dimension: item_information_claim_detail_code_name {
    type: string
    description: "(T4358, T4359) Combination of code_names for claim_type and claim_element, e.g. Fri från Gluten, Låg Laktos"
    sql: ${TABLE}.item_information_claim_detail_code_name ;;
  }
  dimension: item_information_claim_detail_code_value {
    type: string
    description: "(T4358, T4359) Combination of code_values for claim_type and claim_element, e.g. FREE_FROM GLUTEN, LOW_ON LACTOSE"
    sql: ${TABLE}.item_information_claim_detail_code_value ;;
  }
}

view: d_item_v3__ica_environmental_accreditation {

  dimension: d_item_v3__ica_environmental_accreditation {
    type: string
    description: "(T3777) Item accreditations considered as environmental by ICA, see detail on BICA wiki, subset of accredition-attribute"
    hidden: yes
    sql: d_item_v3__ica_environmental_accreditation ;;
  }
  dimension: ica_environmental_accreditation_code {
    type: string
    sql: ${TABLE}.ica_environmental_accreditation_code ;;
  }
  dimension: ica_environmental_accreditation_description {
    type: string
    sql: ${TABLE}.ica_environmental_accreditation_description ;;
  }
  dimension: ica_environmental_accreditation_name {
    type: string
    sql: ${TABLE}.ica_environmental_accreditation_name ;;
  }
}

view: d_item_v3__ica_non_ecological_accreditation {

  dimension: d_item_v3__ica_non_ecological_accreditation {
    type: string
    description: "(T3777) Item accreditations considered as environmental and non-ecological by ICA, see detail on BICA wiki, subset of accredition-attribute"
    hidden: yes
    sql: d_item_v3__ica_non_ecological_accreditation ;;
  }
  dimension: ica_non_ecological_accreditation_code {
    type: string
    sql: ${TABLE}.ica_non_ecological_accreditation_code ;;
  }
  dimension: ica_non_ecological_accreditation_description {
    type: string
    sql: ${TABLE}.ica_non_ecological_accreditation_description ;;
  }
  dimension: ica_non_ecological_accreditation_name {
    type: string
    sql: ${TABLE}.ica_non_ecological_accreditation_name ;;
  }
}

view: d_item_v3__packaging_information__packaging_material_composition {

  dimension: packaging_material_composition_quantity {
    hidden: yes
    sql: ${TABLE}.packaging_material_composition_quantity ;;
  }
  dimension: packaging_material_type__code_description {
    type: string
    description: "The materials used for the packaging of the trade item for example glass or plastic. This material information can be used by data recipients for; o Tax calculations/fees/duties calculation o Carbon footprint calculations/estimations (resource optimisation) o to determine the material used."
    sql: ${TABLE}.packaging_material_type.code_description ;;
    group_label: "Packaging Material Type"
    group_item_label: "Code Description"
  }
  dimension: packaging_material_type__code_name {
    type: string
    description: "The materials used for the packaging of the trade item for example glass or plastic. This material information can be used by data recipients for; o Tax calculations/fees/duties calculation o Carbon footprint calculations/estimations (resource optimisation) o to determine the material used."
    sql: ${TABLE}.packaging_material_type.code_name ;;
    group_label: "Packaging Material Type"
    group_item_label: "Code Name"
  }
  dimension: packaging_material_type__code_value {
    type: string
    description: "The materials used for the packaging of the trade item for example glass or plastic. This material information can be used by data recipients for; o Tax calculations/fees/duties calculation o Carbon footprint calculations/estimations (resource optimisation) o to determine the material used."
    sql: ${TABLE}.packaging_material_type.code_value ;;
    group_label: "Packaging Material Type"
    group_item_label: "Code Value"
  }
}

view: d_item_v3__packaging_information__packaging_material_composition__packaging_material_composition_quantity {

  dimension: quantity_unit_of_measure {
    type: string
    description: "The Unit Of Measure for the PackagingMaterialCompositionQuantity attribute."
    sql: ${TABLE}.quantity_unit_of_measure ;;
  }
  dimension: quantity_value {
    type: number
    description: "The quantity of the packaging material of the trade item. Can be weight, volume or surface, can vary by country."
    sql: ${TABLE}.quantity_value ;;
  }
}

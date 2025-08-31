view: d_item_v3 {
  sql_table_name: `ac16-p-conlaybi-prd-4257`.`item_versioned`.`d_item_v3` ;;

  dimension: d_item_key {
    type: number
    sql: ${TABLE}.d_item_key ;;
  }

  dimension: item_id {
    type: string
    sql: ${TABLE}.item_id ;;
  }

  dimension: global_trade_item_number {
    type: string
    sql: ${TABLE}.global_trade_item_number ;;
  }

  dimension: item_reporting_description {
    type: string
    sql: ${TABLE}.item_reporting_description ;;
  }

  dimension: item_description {
    type: string
    sql: ${TABLE}.item_description ;;
  }

  dimension: item_pack_type {
    type: string
    sql: ${TABLE}.item_pack_type ;;
  }

  dimension: is_base_unit {
    type: yesno
    sql: ${TABLE}.is_base_unit ;;
  }

  dimension: is_consumer_unit {
    type: yesno
    sql: ${TABLE}.is_consumer_unit ;;
  }

  dimension: is_orderable_unit {
    type: yesno
    sql: ${TABLE}.is_orderable_unit ;;
  }

  dimension: is_invoice_unit {
    type: yesno
    sql: ${TABLE}.is_invoice_unit ;;
  }

  dimension: is_despatch_unit {
    type: yesno
    sql: ${TABLE}.is_despatch_unit ;;
  }

  dimension: is_ica_external_sourcing {
    type: yesno
    sql: ${TABLE}.is_ica_external_sourcing ;;
  }

  dimension: consumer_item_reference__consumer_item_id {
    type: string
    sql: ${TABLE}.consumer_item_reference.consumer_item_id ;;
    group_label: "Consumer item reference"
    group_item_label: "Consumer item id"
  }

  dimension: consumer_item_reference__plu_number {
    type: string
    sql: ${TABLE}.consumer_item_reference.plu_number ;;
    group_label: "Consumer item reference"
    group_item_label: "Plu number"
  }

  dimension: is_bonus_item {
    type: yesno
    sql: ${TABLE}.is_bonus_item ;;
  }

  dimension: is_scale_plu {
    type: yesno
    sql: ${TABLE}.is_scale_plu ;;
  }

  dimension: is_private_label {
    type: yesno
    sql: ${TABLE}.is_private_label ;;
  }

  dimension: is_corporate_brand {
    type: yesno
    sql: ${TABLE}.is_corporate_brand ;;
  }

  dimension: brand__code_value {
    type: string
    sql: ${TABLE}.brand.code_value ;;
    group_label: "Brand"
    group_item_label: "Code value"
  }

  dimension: brand__code_name {
    type: string
    sql: ${TABLE}.brand.code_name ;;
    group_label: "Brand"
    group_item_label: "Code name"
  }

  dimension: brand__code_description {
    type: string
    sql: ${TABLE}.brand.code_description ;;
    group_label: "Brand"
    group_item_label: "Code description"
  }

  dimension: country_of_origin {
    sql: ${TABLE}.country_of_origin ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: division_id {
    type: string
    sql: ${TABLE}.division_id ;;
  }

  dimension: division_name {
    type: string
    sql: ${TABLE}.division_name ;;
  }

  dimension: division_description {
    type: string
    sql: ${TABLE}.division_description ;;
  }

  dimension: main_category_id {
    type: string
    sql: ${TABLE}.main_category_id ;;
  }

  dimension: main_category_name {
    type: string
    sql: ${TABLE}.main_category_name ;;
  }

  dimension: main_category_description {
    type: string
    sql: ${TABLE}.main_category_description ;;
  }

  dimension: category_id {
    type: string
    sql: ${TABLE}.category_id ;;
  }

  dimension: category_name {
    type: string
    sql: ${TABLE}.category_name ;;
  }

  dimension: category_description {
    type: string
    sql: ${TABLE}.category_description ;;
  }

  dimension: sub_category_id {
    type: string
    sql: ${TABLE}.sub_category_id ;;
  }

  dimension: sub_category_name {
    type: string
    sql: ${TABLE}.sub_category_name ;;
  }

  dimension: sub_category_description {
    type: string
    sql: ${TABLE}.sub_category_description ;;
  }

  dimension: segment_id {
    type: string
    sql: ${TABLE}.segment_id ;;
  }

  dimension: segment_name {
    type: string
    sql: ${TABLE}.segment_name ;;
  }

  dimension: segment_description {
    type: string
    sql: ${TABLE}.segment_description ;;
  }

  dimension: css_main_category_group_id {
    type: string
    sql: ${TABLE}.css_main_category_group_id ;;
  }

  dimension: css_main_category_group_name {
    type: string
    sql: ${TABLE}.css_main_category_group_name ;;
  }

  dimension: css_main_category_group_description {
    type: string
    sql: ${TABLE}.css_main_category_group_description ;;
  }

  dimension: central_department {
    sql: ${TABLE}.central_department ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: lifecycle__central_status {
    type: string
    sql: ${TABLE}.lifecycle.central_status ;;
    group_label: "Lifecycle"
    group_item_label: "Central status"
  }

  dimension: lifecycle__introduction_status {
    type: string
    sql: ${TABLE}.lifecycle.introduction_status ;;
    group_label: "Lifecycle"
    group_item_label: "Introduction status"
  }

  dimension: lifecycle__novelty_type {
    type: string
    sql: ${TABLE}.lifecycle.novelty_type ;;
    group_label: "Lifecycle"
    group_item_label: "Novelty type"
  }

  dimension: lifecycle__ica_discontinue_reason {
    type: string
    sql: ${TABLE}.lifecycle.ica_discontinue_reason ;;
    group_label: "Lifecycle"
    group_item_label: "Ica discontinue reason"
  }

  dimension: lifecycle__on_hold_reason {
    type: string
    sql: ${TABLE}.lifecycle.on_hold_reason ;;
    group_label: "Lifecycle"
    group_item_label: "On hold reason"
  }

  dimension: assortment_attributes__ecological {
    type: string
    sql: ${TABLE}.assortment_attributes.ecological ;;
    group_label: "Assortment attributes"
    group_item_label: "Ecological"
  }

  dimension: assortment_attributes__environmental {
    type: string
    sql: ${TABLE}.assortment_attributes.environmental ;;
    group_label: "Assortment attributes"
    group_item_label: "Environmental"
  }

  dimension: assortment_attributes__environmental_non_ecological {
    type: string
    sql: ${TABLE}.assortment_attributes.environmental_non_ecological ;;
    group_label: "Assortment attributes"
    group_item_label: "Environmental non ecological"
  }

  dimension: assortment_attributes__ethical {
    type: string
    sql: ${TABLE}.assortment_attributes.ethical ;;
    group_label: "Assortment attributes"
    group_item_label: "Ethical"
  }

  dimension: assortment_attributes__sustainable {
    type: string
    sql: ${TABLE}.assortment_attributes.sustainable ;;
    group_label: "Assortment attributes"
    group_item_label: "Sustainable"
  }

  dimension: assortment_attributes__gdpr_sensitive__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.gdpr_sensitive.code_value ;;
    group_label: "Assortment attributes Gdpr sensitive"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__gdpr_sensitive__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.gdpr_sensitive.code_name ;;
    group_label: "Assortment attributes Gdpr sensitive"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__gdpr_sensitive__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.gdpr_sensitive.code_description ;;
    group_label: "Assortment attributes Gdpr sensitive"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: assortment_attributes__health {
    type: string
    sql: ${TABLE}.assortment_attributes.health ;;
    group_label: "Assortment attributes"
    group_item_label: "Health"
  }

  dimension: assortment_attributes__multicultural__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.multicultural.code_value ;;
    group_label: "Assortment attributes Multicultural"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__multicultural__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.multicultural.code_name ;;
    group_label: "Assortment attributes Multicultural"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__multicultural__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.multicultural.code_description ;;
    group_label: "Assortment attributes Multicultural"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: assortment_attributes__packing_size__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.packing_size.code_value ;;
    group_label: "Assortment attributes Packing size"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__packing_size__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.packing_size.code_name ;;
    group_label: "Assortment attributes Packing size"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__packing_size__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.packing_size.code_description ;;
    group_label: "Assortment attributes Packing size"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: assortment_attributes__pack_variant__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.pack_variant.code_value ;;
    group_label: "Assortment attributes Pack variant"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__pack_variant__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.pack_variant.code_name ;;
    group_label: "Assortment attributes Pack variant"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__pack_variant__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.pack_variant.code_description ;;
    group_label: "Assortment attributes Pack variant"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: assortment_attributes__plantbased__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.plantbased.code_value ;;
    group_label: "Assortment attributes Plantbased"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__plantbased__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.plantbased.code_name ;;
    group_label: "Assortment attributes Plantbased"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__plantbased__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.plantbased.code_description ;;
    group_label: "Assortment attributes Plantbased"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: assortment_attributes__price_range__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.price_range.code_value ;;
    group_label: "Assortment attributes Price range"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__price_range__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.price_range.code_name ;;
    group_label: "Assortment attributes Price range"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__price_range__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.price_range.code_description ;;
    group_label: "Assortment attributes Price range"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: assortment_attributes__quality__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.quality.code_value ;;
    group_label: "Assortment attributes Quality"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__quality__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.quality.code_name ;;
    group_label: "Assortment attributes Quality"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__quality__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.quality.code_description ;;
    group_label: "Assortment attributes Quality"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: assortment_attributes__swedish__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.swedish.code_value ;;
    group_label: "Assortment attributes Swedish"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__swedish__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.swedish.code_name ;;
    group_label: "Assortment attributes Swedish"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__swedish__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.swedish.code_description ;;
    group_label: "Assortment attributes Swedish"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__colour__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.colour.code_value ;;
    group_label: "Category specific attributes Colour"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__colour__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.colour.code_name ;;
    group_label: "Category specific attributes Colour"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__colour__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.colour.code_description ;;
    group_label: "Category specific attributes Colour"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__consumer_group__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.consumer_group.code_value ;;
    group_label: "Category specific attributes Consumer group"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__consumer_group__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.consumer_group.code_name ;;
    group_label: "Category specific attributes Consumer group"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__consumer_group__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.consumer_group.code_description ;;
    group_label: "Category specific attributes Consumer group"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__execution1__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution1.code_value ;;
    group_label: "Category specific attributes Execution1"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__execution1__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution1.code_name ;;
    group_label: "Category specific attributes Execution1"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__execution1__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution1.code_description ;;
    group_label: "Category specific attributes Execution1"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__execution2__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution2.code_value ;;
    group_label: "Category specific attributes Execution2"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__execution2__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution2.code_name ;;
    group_label: "Category specific attributes Execution2"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__execution2__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution2.code_description ;;
    group_label: "Category specific attributes Execution2"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__execution3__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution3.code_value ;;
    group_label: "Category specific attributes Execution3"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__execution3__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution3.code_name ;;
    group_label: "Category specific attributes Execution3"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__execution3__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution3.code_description ;;
    group_label: "Category specific attributes Execution3"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__execution4__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution4.code_value ;;
    group_label: "Category specific attributes Execution4"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__execution4__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution4.code_name ;;
    group_label: "Category specific attributes Execution4"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__execution4__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.execution4.code_description ;;
    group_label: "Category specific attributes Execution4"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__flavour__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.flavour.code_value ;;
    group_label: "Category specific attributes Flavour"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__flavour__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.flavour.code_name ;;
    group_label: "Category specific attributes Flavour"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__flavour__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.flavour.code_description ;;
    group_label: "Category specific attributes Flavour"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__origin__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.origin.code_value ;;
    group_label: "Category specific attributes Origin"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__origin__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.origin.code_name ;;
    group_label: "Category specific attributes Origin"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__origin__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.origin.code_description ;;
    group_label: "Category specific attributes Origin"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__preparation__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.preparation.code_value ;;
    group_label: "Category specific attributes Preparation"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__preparation__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.preparation.code_name ;;
    group_label: "Category specific attributes Preparation"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__preparation__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.preparation.code_description ;;
    group_label: "Category specific attributes Preparation"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__product_group__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.product_group.code_value ;;
    group_label: "Category specific attributes Product group"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__product_group__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.product_group.code_name ;;
    group_label: "Category specific attributes Product group"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__product_group__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.product_group.code_description ;;
    group_label: "Category specific attributes Product group"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__raw_material__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.raw_material.code_value ;;
    group_label: "Category specific attributes Raw material"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__raw_material__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.raw_material.code_name ;;
    group_label: "Category specific attributes Raw material"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__raw_material__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.raw_material.code_description ;;
    group_label: "Category specific attributes Raw material"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: category_specific_attributes__specific_content__code_value {
    type: string
    sql: ${TABLE}.category_specific_attributes.specific_content.code_value ;;
    group_label: "Category specific attributes Specific content"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: category_specific_attributes__specific_content__code_name {
    type: string
    sql: ${TABLE}.category_specific_attributes.specific_content.code_name ;;
    group_label: "Category specific attributes Specific content"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: category_specific_attributes__specific_content__code_description {
    type: string
    sql: ${TABLE}.category_specific_attributes.specific_content.code_description ;;
    group_label: "Category specific attributes Specific content"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: accreditation {
    sql: ${TABLE}.accreditation ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: ica_ethical_accreditation {
    sql: ${TABLE}.ica_ethical_accreditation ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: ica_environmental_accreditation {
    sql: ${TABLE}.ica_environmental_accreditation ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: ica_ecological_accreditation {
    sql: ${TABLE}.ica_ecological_accreditation ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: ica_non_ecological_accreditation {
    sql: ${TABLE}.ica_non_ecological_accreditation ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: core_input_reason__code_value {
    type: string
    sql: ${TABLE}.core_input_reason.code_value ;;
    group_label: "Core input reason"
    group_item_label: "Code value"
  }

  dimension: core_input_reason__code_name {
    type: string
    sql: ${TABLE}.core_input_reason.code_name ;;
    group_label: "Core input reason"
    group_item_label: "Code name"
  }

  dimension: core_input_reason__code_description {
    type: string
    sql: ${TABLE}.core_input_reason.code_description ;;
    group_label: "Core input reason"
    group_item_label: "Code description"
  }

  dimension: is_seasonal {
    type: yesno
    sql: ${TABLE}.is_seasonal ;;
  }

  dimension: season__code_value {
    type: string
    sql: ${TABLE}.season.code_value ;;
    group_label: "Season"
    group_item_label: "Code value"
  }

  dimension: season__code_name {
    type: string
    sql: ${TABLE}.season.code_name ;;
    group_label: "Season"
    group_item_label: "Code name"
  }

  dimension: season__code_description {
    type: string
    sql: ${TABLE}.season.code_description ;;
    group_label: "Season"
    group_item_label: "Code description"
  }

  dimension: ecr_category__code_value {
    type: string
    sql: ${TABLE}.ecr_category.code_value ;;
    group_label: "Ecr category"
    group_item_label: "Code value"
  }

  dimension: ecr_category__code_name {
    type: string
    sql: ${TABLE}.ecr_category.code_name ;;
    group_label: "Ecr category"
    group_item_label: "Code name"
  }

  dimension: ecr_category__code_description {
    type: string
    sql: ${TABLE}.ecr_category.code_description ;;
    group_label: "Ecr category"
    group_item_label: "Code description"
  }

  dimension: is_catchweight_item {
    type: yesno
    sql: ${TABLE}.is_catchweight_item ;;
  }

  dimension: catchweight_type_cd {
    type: string
    sql: ${TABLE}.catchweight_type_cd ;;
  }

  dimension: descriptive_size {
    type: string
    sql: ${TABLE}.descriptive_size ;;
  }

  dimension: functional_name {
    type: string
    sql: ${TABLE}.functional_name ;;
  }

  dimension: supply_chain_orderable_status {
    type: string
    sql: ${TABLE}.supply_chain_orderable_status ;;
  }

  dimension: net_weight {
    type: number
    sql: ${TABLE}.net_weight ;;
  }

  dimension: standard_unit_of_measure {
    type: string
    sql: ${TABLE}.standard_unit_of_measure ;;
  }

  dimension: vat_percent {
    type: number
    sql: ${TABLE}.vat_percent ;;
  }

  dimension: measurements__depth {
    type: number
    sql: ${TABLE}.measurements.depth ;;
    group_label: "Measurements"
    group_item_label: "Depth"
  }

  dimension: measurements__depth_unit_of_measure {
    type: string
    sql: ${TABLE}.measurements.depth_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Depth unit of measure"
  }

  dimension: measurements__height {
    type: number
    sql: ${TABLE}.measurements.height ;;
    group_label: "Measurements"
    group_item_label: "Height"
  }

  dimension: measurements__height_unit_of_measure {
    type: string
    sql: ${TABLE}.measurements.height_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Height unit of measure"
  }

  dimension: measurements__width {
    type: number
    sql: ${TABLE}.measurements.width ;;
    group_label: "Measurements"
    group_item_label: "Width"
  }

  dimension: measurements__width_unit_of_measure {
    type: string
    sql: ${TABLE}.measurements.width_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Width unit of measure"
  }

  dimension: measurements__net_content_in_gram {
    type: number
    sql: ${TABLE}.measurements.net_content_in_gram ;;
    group_label: "Measurements"
    group_item_label: "Net content in gram"
  }

  dimension: measurements__net_content_per_piece {
    type: number
    sql: ${TABLE}.measurements.net_content_per_piece ;;
    group_label: "Measurements"
    group_item_label: "Net content per piece"
  }

  dimension: measurements__net_content_in_litre {
    type: number
    sql: ${TABLE}.measurements.net_content_in_litre ;;
    group_label: "Measurements"
    group_item_label: "Net content in litre"
  }

  dimension: measurements__net_content_in_millilitre {
    type: number
    sql: ${TABLE}.measurements.net_content_in_millilitre ;;
    group_label: "Measurements"
    group_item_label: "Net content in millilitre"
  }

  dimension: measurements__net_content_in_millimeter {
    type: number
    sql: ${TABLE}.measurements.net_content_in_millimeter ;;
    group_label: "Measurements"
    group_item_label: "Net content in millimeter"
  }

  dimension: measurements__net_content_in_kilogram {
    type: number
    sql: ${TABLE}.measurements.net_content_in_kilogram ;;
    group_label: "Measurements"
    group_item_label: "Net content in kilogram"
  }

  dimension: measurements__net_content_others {
    type: number
    sql: ${TABLE}.measurements.net_content_others ;;
    group_label: "Measurements"
    group_item_label: "Net content others"
  }

  dimension: measurements__net_content_others_unit_of_measure {
    type: string
    sql: ${TABLE}.measurements.net_content_others_unit_of_measure ;;
    group_label: "Measurements"
    group_item_label: "Net content others unit of measure"
  }

  dimension: measurements__gross_weight_in_gram {
    type: number
    sql: ${TABLE}.measurements.gross_weight_in_gram ;;
    group_label: "Measurements"
    group_item_label: "Gross weight in gram"
  }

  dimension: information_providing_supplier {
    type: string
    sql: ${TABLE}.information_providing_supplier ;;
  }

  dimension: price_comparison {
    type: number
    sql: ${TABLE}.price_comparison ;;
  }

  dimension: price_comparison_unit_of_measure {
    type: string
    sql: ${TABLE}.price_comparison_unit_of_measure ;;
  }

  dimension: gpc_category_code {
    type: string
    sql: ${TABLE}.gpc_category_code ;;
  }

  dimension: gpc_category_definition {
    type: string
    sql: ${TABLE}.gpc_category_definition ;;
  }

  dimension: gpc_category_name {
    type: string
    sql: ${TABLE}.gpc_category_name ;;
  }

  dimension: alcohol_percentage_by_volume {
    type: number
    sql: ${TABLE}.alcohol_percentage_by_volume ;;
  }

  dimension: packaging_information__packaging_weight {
    type: number
    sql: ${TABLE}.packaging_information.packaging_weight ;;
    group_label: "Packaging information"
    group_item_label: "Packaging weight"
  }

  dimension: packaging_information__packaging_weight_uom {
    type: string
    sql: ${TABLE}.packaging_information.packaging_weight_uom ;;
    group_label: "Packaging information"
    group_item_label: "Packaging weight uom"
  }

  dimension: packaging_information__packaging_material_composition {
    sql: ${TABLE}.packaging_information.packaging_material_composition ;;
    group_label: "Packaging information"
    group_item_label: "Packaging material composition"
    hidden: yes
    tags: ["array"]
  }

  dimension: bica_calculated_fields__bica_improved_weight_volume {
    type: number
    sql: ${TABLE}.bica_calculated_fields.bica_improved_weight_volume ;;
    group_label: "Bica calculated fields"
    group_item_label: "Bica improved weight volume"
  }

  dimension: bica_calculated_fields__bica_improved_weight_volume_uom {
    type: string
    sql: ${TABLE}.bica_calculated_fields.bica_improved_weight_volume_uom ;;
    group_label: "Bica calculated fields"
    group_item_label: "Bica improved weight volume uom"
  }

  dimension: bica_calculated_fields__bica_improved_ecological_markup {
    type: string
    sql: ${TABLE}.bica_calculated_fields.bica_improved_ecological_markup ;;
    group_label: "Bica calculated fields"
    group_item_label: "Bica improved ecological markup"
  }

  dimension: primary_soi_supplier_reference__store_orderable_item_id {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.store_orderable_item_id ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Store orderable item id"
  }

  dimension: primary_soi_supplier_reference__soi_description {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.soi_description ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Soi description"
  }

  dimension: primary_soi_supplier_reference__is_primary_consumer_item_for_soi {
    type: yesno
    sql: ${TABLE}.primary_soi_supplier_reference.is_primary_consumer_item_for_soi ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Is primary consumer item for soi"
  }

  dimension: primary_soi_supplier_reference__soi_status {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.soi_status ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Soi status"
  }

  dimension: primary_soi_supplier_reference__supplychain_supplier_id {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.supplychain_supplier_id ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Supplychain supplier id"
  }

  dimension: primary_soi_supplier_reference__supplychain_supplier_short_name {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.supplychain_supplier_short_name ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Supplychain supplier short name"
  }

  dimension: primary_soi_supplier_reference__supplychain_supplier_long_name {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.supplychain_supplier_long_name ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Supplychain supplier long name"
  }

  dimension: primary_soi_supplier_reference__supplier_site_id {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_site_id ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Supplier site id"
  }

  dimension: primary_soi_supplier_reference__supplier_site_description {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_site_description ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Supplier site description"
  }

  dimension: primary_soi_supplier_reference__supplier_id {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_id ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Supplier id"
  }

  dimension: primary_soi_supplier_reference__supplier_organization_name {
    type: string
    sql: ${TABLE}.primary_soi_supplier_reference.supplier_organization_name ;;
    group_label: "Primary soi supplier reference"
    group_item_label: "Supplier organization name"
  }

  dimension: item_information_claim_detail {
    sql: ${TABLE}.item_information_claim_detail ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: returnable_asset_deposit_type {
    type: string
    sql: ${TABLE}.returnable_asset_deposit_type ;;
  }

  dimension: returnable_asset_deposit_name {
    type: string
    sql: ${TABLE}.returnable_asset_deposit_name ;;
  }

  dimension: aggregated_base_item_quantity {
    type: number
    sql: ${TABLE}.aggregated_base_item_quantity ;;
  }

  dimension: aggregated_deposit_amount {
    type: number
    sql: ${TABLE}.aggregated_deposit_amount ;;
  }

  dimension: load_carrier_deposit {
    sql: ${TABLE}.load_carrier_deposit ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: md_row_hash {
    type: number
    sql: ${TABLE}.md_row_hash ;;
  }

  dimension: md_audit_seq {
    type: string
    sql: ${TABLE}.md_audit_seq ;;
  }

  dimension: item_reporting_id {
    type: string
    sql: ${TABLE}.item_reporting_id ;;
  }

  dimension: assortment_attributes__ica_swedish__code_value {
    type: string
    sql: ${TABLE}.assortment_attributes.ica_swedish.code_value ;;
    group_label: "Assortment attributes Ica swedish"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: assortment_attributes__ica_swedish__code_name {
    type: string
    sql: ${TABLE}.assortment_attributes.ica_swedish.code_name ;;
    group_label: "Assortment attributes Ica swedish"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: assortment_attributes__ica_swedish__code_description {
    type: string
    sql: ${TABLE}.assortment_attributes.ica_swedish.code_description ;;
    group_label: "Assortment attributes Ica swedish"
    group_item_label: "Code description"
    hidden: yes
  }

  dimension: ica_swedish_accreditation {
    sql: ${TABLE}.ica_swedish_accreditation ;;
    hidden: yes
    tags: ["array"]
  }

  dimension_group: lifecycle__creation_datetime {
    label: "Creation Datetime"
    type: time
    sql: ${TABLE}.lifecycle.creation_datetime ;;
    datatype: datetime
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: yes
    description: "Automatic time stamp when item is created"
  }

  dimension_group: lifecycle__novelty_start_date {
    label: "Novelty Start Date"
    type: time
    sql: ${TABLE}.lifecycle.novelty_start_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: no
    description: "Startdate when item is considered as new"
  }

  dimension_group: lifecycle__novelty_end_date {
    label: "Novelty End Date"
    type: time
    sql: ${TABLE}.lifecycle.novelty_end_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: no
    description: "Enddate when item is considered as new"
  }

  dimension_group: lifecycle__ica_discontinue_date {
    label: "Ica Discontinue Date"
    type: time
    sql: ${TABLE}.lifecycle.ica_discontinue_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: no
    description: "When ICA decides to discontinue an item. If the attributes is empty (an ICA internal decision is made before supplier sends in information), the discontinue date shall be copied from GS1 attribute."
  }

  dimension_group: lifecycle__on_hold_start_date {
    label: "On Hold Start Date"
    type: time
    sql: ${TABLE}.lifecycle.on_hold_start_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: no
    description: "Start date, when item no longer is active in assortment."
  }

  dimension_group: lifecycle__obsolete_date {
    label: "Obsolete Date"
    type: time
    sql: ${TABLE}.lifecycle.obsolete_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: no
    description: "The date when the item record goes into status Obsolete"
  }

  dimension_group: lifecycle__purge_date {
    label: "Purge Date"
    type: time
    sql: ${TABLE}.lifecycle.purge_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: no
    description: "The date when the item record is to be removed from the FPH db"
  }

  dimension_group: lifecycle__reactivation_date {
    label: "Reactivation Date"
    type: time
    sql: ${TABLE}.lifecycle.reactivation_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Lifecycle"
    convert_tz: no
    description: "The date when the item will be reactivated"
  }

  dimension_group: season_start {
    label: "Season Start Date"
    type: time
    sql: ${TABLE}.season_start_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Season Start Date"
    convert_tz: no
    description: "The start date for the season"
  }

  dimension_group: season_end {
    label: "Season End Date"
    type: time
    sql: ${TABLE}.season_end_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Season End Date"
    convert_tz: no
    description: "The end date for the season"
  }

  dimension_group: ecr_revision {
    label: "Ecr Revision Date"
    type: time
    sql: ${TABLE}.ecr_revision_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Ecr Revision Date"
    convert_tz: no
    description: "Launch date (FPH/ECR Calander) chosen by the supplier in the product portal. Category Manager can change date"
  }

  dimension_group: primary_soi_supplier_reference__delivery_start_date {
    label: "Delivery Start Date"
    type: time
    sql: ${TABLE}.primary_soi_supplier_reference.delivery_start_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Primary soi supplier reference"
    convert_tz: no
    description: "Delivery Start Date is when the SOI is deliverable to stores"
  }

  dimension_group: primary_soi_supplier_reference__orderability_start_date {
    label: "Orderability Start Date"
    type: time
    sql: ${TABLE}.primary_soi_supplier_reference.orderability_start_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Primary soi supplier reference"
    convert_tz: no
    description: "Orderability is when the SOI is orderable for stores."
  }

  dimension_group: primary_soi_supplier_reference__orderability_end_date {
    label: "Orderability End Date"
    type: time
    sql: ${TABLE}.primary_soi_supplier_reference.orderability_end_date ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Primary soi supplier reference"
    convert_tz: no
    description: "Orderability is when the SOI is orderable for stores."
  }

  dimension_group: md_insert_dttm {
    label: "Md Insert Dttm"
    type: time
    sql: ${TABLE}.md_insert_dttm ;;
    datatype: datetime
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Md Insert Dttm"
    convert_tz: yes
    description: "Technical field insert datettime"
  }
}

view: d_item_v3__country_of_origin {
  dimension: country_of_origin {
    type: string
    hidden: yes
    sql: ${d_item_v3__country_of_origin} ;;
  }
}

view: d_item_v3__central_department {
  dimension: d_item_v3__central_department {
    type: string
    hidden: yes
    sql: d_item_v3__central_department ;;
    description: "Department (used for central analysis close to store, maintained by Store and Marketing sponsor area)"
  }

  dimension: profile_id {
    type: string
    sql: ${TABLE}.central_department.profile_id ;;
    group_label: "Central department"
    group_item_label: "Profile id"
  }

  dimension: profile_name {
    type: string
    sql: ${TABLE}.central_department.profile_name ;;
    group_label: "Central department"
    group_item_label: "Profile name"
  }

  dimension: central_department_code {
    type: string
    sql: ${TABLE}.central_department.central_department_code ;;
    group_label: "Central department"
    group_item_label: "Central department code"
  }

  dimension: central_department_name {
    type: string
    sql: ${TABLE}.central_department.central_department_name ;;
    group_label: "Central department"
    group_item_label: "Central department name"
  }

  dimension: central_department_description {
    type: string
    sql: ${TABLE}.central_department.central_department_description ;;
    group_label: "Central department"
    group_item_label: "Central department description"
  }
}

view: d_item_v3__accreditation {
  dimension: d_item_v3__accreditation {
    type: string
    hidden: yes
    sql: d_item_v3__accreditation ;;
    description: "(T3777) All item acceditations (GS1 CodeList PackagingMarkedLabelAccreditationCode)"
  }

  dimension: accreditation_code {
    type: string
    sql: ${TABLE}.accreditation.accreditation_code ;;
    group_label: "Accreditation"
    group_item_label: "Accreditation code"
  }

  dimension: accreditation_name {
    type: string
    sql: ${TABLE}.accreditation.accreditation_name ;;
    group_label: "Accreditation"
    group_item_label: "Accreditation name"
  }

  dimension: accreditation_description {
    type: string
    sql: ${TABLE}.accreditation.accreditation_description ;;
    group_label: "Accreditation"
    group_item_label: "Accreditation description"
  }
}

view: d_item_v3__ica_ethical_accreditation {
  dimension: d_item_v3__ica_ethical_accreditation {
    type: string
    hidden: yes
    sql: d_item_v3__ica_ethical_accreditation ;;
    description: "(T3777) Item accreditations considered as ethical by ICA, see detail on BICA wiki, subset of accredition-attribute"
  }

  dimension: ica_ethical_accreditation_code {
    type: string
    sql: ${TABLE}.ica_ethical_accreditation.ica_ethical_accreditation_code ;;
    group_label: "Ica ethical accreditation"
    group_item_label: "Ica ethical accreditation code"
  }

  dimension: ica_ethical_accreditation_name {
    type: string
    sql: ${TABLE}.ica_ethical_accreditation.ica_ethical_accreditation_name ;;
    group_label: "Ica ethical accreditation"
    group_item_label: "Ica ethical accreditation name"
  }

  dimension: ica_ethical_accreditation_description {
    type: string
    sql: ${TABLE}.ica_ethical_accreditation.ica_ethical_accreditation_description ;;
    group_label: "Ica ethical accreditation"
    group_item_label: "Ica ethical accreditation description"
  }
}

view: d_item_v3__ica_environmental_accreditation {
  dimension: d_item_v3__ica_environmental_accreditation {
    type: string
    hidden: yes
    sql: d_item_v3__ica_environmental_accreditation ;;
    description: "(T3777) Item accreditations considered as environmental by ICA, see detail on BICA wiki, subset of accredition-attribute"
  }

  dimension: ica_environmental_accreditation_code {
    type: string
    sql: ${TABLE}.ica_environmental_accreditation.ica_environmental_accreditation_code ;;
    group_label: "Ica environmental accreditation"
    group_item_label: "Ica environmental accreditation code"
  }

  dimension: ica_environmental_accreditation_name {
    type: string
    sql: ${TABLE}.ica_environmental_accreditation.ica_environmental_accreditation_name ;;
    group_label: "Ica environmental accreditation"
    group_item_label: "Ica environmental accreditation name"
  }

  dimension: ica_environmental_accreditation_description {
    type: string
    sql: ${TABLE}.ica_environmental_accreditation.ica_environmental_accreditation_description ;;
    group_label: "Ica environmental accreditation"
    group_item_label: "Ica environmental accreditation description"
  }
}

view: d_item_v3__ica_ecological_accreditation {
  dimension: d_item_v3__ica_ecological_accreditation {
    type: string
    hidden: yes
    sql: d_item_v3__ica_ecological_accreditation ;;
    description: "(T3777) Item accreditations considered as environmental and ecological by ICA, see detail on BICA wiki, subset of accredition-attribute"
  }

  dimension: ica_ecological_accreditation_code {
    type: string
    sql: ${TABLE}.ica_ecological_accreditation.ica_ecological_accreditation_code ;;
    group_label: "Ica ecological accreditation"
    group_item_label: "Ica ecological accreditation code"
  }

  dimension: ica_ecological_accreditation_name {
    type: string
    sql: ${TABLE}.ica_ecological_accreditation.ica_ecological_accreditation_name ;;
    group_label: "Ica ecological accreditation"
    group_item_label: "Ica ecological accreditation name"
  }

  dimension: ica_ecological_accreditation_description {
    type: string
    sql: ${TABLE}.ica_ecological_accreditation.ica_ecological_accreditation_description ;;
    group_label: "Ica ecological accreditation"
    group_item_label: "Ica ecological accreditation description"
  }
}

view: d_item_v3__ica_non_ecological_accreditation {
  dimension: d_item_v3__ica_non_ecological_accreditation {
    type: string
    hidden: yes
    sql: d_item_v3__ica_non_ecological_accreditation ;;
    description: "(T3777) Item accreditations considered as environmental and non-ecological by ICA, see detail on BICA wiki, subset of accredition-attribute"
  }

  dimension: ica_non_ecological_accreditation_code {
    type: string
    sql: ${TABLE}.ica_non_ecological_accreditation.ica_non_ecological_accreditation_code ;;
    group_label: "Ica non ecological accreditation"
    group_item_label: "Ica non ecological accreditation code"
  }

  dimension: ica_non_ecological_accreditation_name {
    type: string
    sql: ${TABLE}.ica_non_ecological_accreditation.ica_non_ecological_accreditation_name ;;
    group_label: "Ica non ecological accreditation"
    group_item_label: "Ica non ecological accreditation name"
  }

  dimension: ica_non_ecological_accreditation_description {
    type: string
    sql: ${TABLE}.ica_non_ecological_accreditation.ica_non_ecological_accreditation_description ;;
    group_label: "Ica non ecological accreditation"
    group_item_label: "Ica non ecological accreditation description"
  }
}

view: d_item_v3__packaging_information__packaging_material_composition {
  dimension: packaging_material_composition_quantity {
    type: string
    hidden: yes
    sql: ${TABLE}.packaging_material_composition_quantity ;;
  }

  dimension: packaging_material_composition__packaging_material_type__code_value {
    type: string
    sql: ${TABLE}.packaging_information.packaging_material_composition.packaging_material_type.code_value ;;
    group_label: "Packaging information Packaging material composition Packaging material type"
    group_item_label: "Code value"
    hidden: yes
  }

  dimension: packaging_material_composition__packaging_material_type__code_name {
    type: string
    sql: ${TABLE}.packaging_information.packaging_material_composition.packaging_material_type.code_name ;;
    group_label: "Packaging information Packaging material composition Packaging material type"
    group_item_label: "Code name"
    hidden: yes
  }

  dimension: packaging_material_composition__packaging_material_type__code_description {
    type: string
    sql: ${TABLE}.packaging_information.packaging_material_composition.packaging_material_type.code_description ;;
    group_label: "Packaging information Packaging material composition Packaging material type"
    group_item_label: "Code description"
    hidden: yes
  }
}

view: d_item_v3__packaging_information__packaging_material_composition__packaging_material_composition_quantity {
  dimension: packaging_material_composition__packaging_material_composition_quantity__quantity_unit_of_measure {
    type: string
    sql: ${TABLE}.packaging_information.packaging_material_composition.packaging_material_composition_quantity.quantity_unit_of_measure ;;
    group_label: "Packaging information Packaging material composition Packaging material composition quantity"
    group_item_label: "Quantity unit of measure"
    hidden: yes
  }

  dimension: packaging_material_composition__packaging_material_composition_quantity__quantity_value {
    type: number
    sql: ${TABLE}.packaging_information.packaging_material_composition.packaging_material_composition_quantity.quantity_value ;;
    group_label: "Packaging information Packaging material composition Packaging material composition quantity"
    group_item_label: "Quantity value"
    hidden: yes
  }
}

view: d_item_v3__item_information_claim_detail {
  dimension: d_item_v3__item_information_claim_detail {
    type: string
    hidden: yes
    sql: d_item_v3__item_information_claim_detail ;;
    description: "(T4357, T4358, T4359) Item information claim details"
  }

  dimension: is_item_information_claim_marked_on_package {
    type: yesno
    sql: ${TABLE}.item_information_claim_detail.is_item_information_claim_marked_on_package ;;
    group_label: "Item information claim detail"
    group_item_label: "Is item information claim marked on package"
  }

  dimension: claim_type__claim_type_code_value {
    type: string
    sql: ${TABLE}.item_information_claim_detail.claim_type.claim_type_code_value ;;
    group_label: "Item information claim detail Claim type"
    group_item_label: "Claim type code value"
    hidden: yes
  }

  dimension: claim_type__claim_type_code_name {
    type: string
    sql: ${TABLE}.item_information_claim_detail.claim_type.claim_type_code_name ;;
    group_label: "Item information claim detail Claim type"
    group_item_label: "Claim type code name"
    hidden: yes
  }

  dimension: claim_type__claim_type_code_description {
    type: string
    sql: ${TABLE}.item_information_claim_detail.claim_type.claim_type_code_description ;;
    group_label: "Item information claim detail Claim type"
    group_item_label: "Claim type code description"
    hidden: yes
  }

  dimension: claim_element__claim_element_code_value {
    type: string
    sql: ${TABLE}.item_information_claim_detail.claim_element.claim_element_code_value ;;
    group_label: "Item information claim detail Claim element"
    group_item_label: "Claim element code value"
    hidden: yes
  }

  dimension: claim_element__claim_element_code_name {
    type: string
    sql: ${TABLE}.item_information_claim_detail.claim_element.claim_element_code_name ;;
    group_label: "Item information claim detail Claim element"
    group_item_label: "Claim element code name"
    hidden: yes
  }

  dimension: claim_element__claim_element_code_description {
    type: string
    sql: ${TABLE}.item_information_claim_detail.claim_element.claim_element_code_description ;;
    group_label: "Item information claim detail Claim element"
    group_item_label: "Claim element code description"
    hidden: yes
  }

  dimension: item_information_claim_detail_code_value {
    type: string
    sql: ${TABLE}.item_information_claim_detail.item_information_claim_detail_code_value ;;
    group_label: "Item information claim detail"
    group_item_label: "Item information claim detail code value"
  }

  dimension: item_information_claim_detail_code_name {
    type: string
    sql: ${TABLE}.item_information_claim_detail.item_information_claim_detail_code_name ;;
    group_label: "Item information claim detail"
    group_item_label: "Item information claim detail code name"
  }
}

view: d_item_v3__load_carrier_deposit {
  dimension: d_item_v3__load_carrier_deposit {
    type: string
    hidden: yes
    sql: d_item_v3__load_carrier_deposit ;;
    description: "(Record) returnable asset details"
  }

  dimension: returnable_asset_deposit_type {
    type: string
    sql: ${TABLE}.load_carrier_deposit.returnable_asset_deposit_type ;;
    group_label: "Load carrier deposit"
    group_item_label: "Returnable asset deposit type"
  }

  dimension: returnable_asset_contained_quantity {
    type: number
    sql: ${TABLE}.load_carrier_deposit.returnable_asset_contained_quantity ;;
    group_label: "Load carrier deposit"
    group_item_label: "Returnable asset contained quantity"
  }

  dimension: returnable_package_deposit_amount {
    type: number
    sql: ${TABLE}.load_carrier_deposit.returnable_package_deposit_amount ;;
    group_label: "Load carrier deposit"
    group_item_label: "Returnable package deposit amount"
  }

  dimension: returnable_asset_deposit_name {
    type: string
    sql: ${TABLE}.load_carrier_deposit.returnable_asset_deposit_name ;;
    group_label: "Load carrier deposit"
    group_item_label: "Returnable asset deposit name"
  }

  dimension: base_item_quantity {
    type: number
    sql: ${TABLE}.load_carrier_deposit.base_item_quantity ;;
    group_label: "Load carrier deposit"
    group_item_label: "Base item quantity"
  }

  dimension: deposit_amount {
    type: number
    sql: ${TABLE}.load_carrier_deposit.deposit_amount ;;
    group_label: "Load carrier deposit"
    group_item_label: "Deposit amount"
  }
}

view: d_item_v3__ica_swedish_accreditation {
  dimension: d_item_v3__ica_swedish_accreditation {
    type: string
    hidden: yes
    sql: d_item_v3__ica_swedish_accreditation ;;
    description: "(T3777) Item accreditations considered as swedish by ICA, see detail on BICA wiki, subset of accredition-attribute"
  }

  dimension: ica_swedish_accreditation_code {
    type: string
    sql: ${TABLE}.ica_swedish_accreditation.ica_swedish_accreditation_code ;;
    group_label: "Ica swedish accreditation"
    group_item_label: "Ica swedish accreditation code"
  }

  dimension: ica_swedish_accreditation_name {
    type: string
    sql: ${TABLE}.ica_swedish_accreditation.ica_swedish_accreditation_name ;;
    group_label: "Ica swedish accreditation"
    group_item_label: "Ica swedish accreditation name"
  }

  dimension: ica_swedish_accreditation_description {
    type: string
    sql: ${TABLE}.ica_swedish_accreditation.ica_swedish_accreditation_description ;;
    group_label: "Ica swedish accreditation"
    group_item_label: "Ica swedish accreditation description"
  }
}
view: dq_icasoi_current {
  sql_table_name: `ac16-p-conlaybi-prd-4257.item_dataquality.dq_ICASOI_Current` ;;
 
  dimension: buying_item_gtin {
    type: string
    sql: ${TABLE}.BuyingItem_GTIN ;;
  }

  dimension: buying_item_primary {
    type: yesno
    sql: ${TABLE}.BuyingItem_Primary ;;
  }

  dimension: classification__assortment__code {
    type: string
    sql: ${TABLE}.Classification.Assortment.Code ;;
    group_label: "Classification Assortment"
    group_item_label: "Code"
  }

  dimension: classification__assortment__description {
    type: string
    sql: ${TABLE}.Classification.Assortment.Description ;;
    group_label: "Classification Assortment"
    group_item_label: "Description"
  }

  dimension: classification__item_group__code {
    type: string
    sql: ${TABLE}.Classification.ItemGroup.Code ;;
    group_label: "Classification Item Group"
    group_item_label: "Code"
  }

  dimension: classification__item_group__description {
    type: string
    sql: ${TABLE}.Classification.ItemGroup.Description ;;
    group_label: "Classification Item Group"
    group_item_label: "Description"
  }

  dimension: classification__item_sub_group__code {
    type: string
    sql: ${TABLE}.Classification.ItemSubGroup.Code ;;
    group_label: "Classification Item Sub Group"
    group_item_label: "Code"
  }

  dimension: classification__item_sub_group__description {
    type: string
    sql: ${TABLE}.Classification.ItemSubGroup.Description ;;
    group_label: "Classification Item Sub Group"
    group_item_label: "Description"
  }

  dimension: classification__product_class__code {
    type: string
    sql: ${TABLE}.Classification.ProductClass.Code ;;
    group_label: "Classification Product Class"
    group_item_label: "Code"
  }

  dimension: classification__product_class__description {
    type: string
    sql: ${TABLE}.Classification.ProductClass.Description ;;
    group_label: "Classification Product Class"
    group_item_label: "Description"
  }

  dimension: classification__product_group__code {
    type: string
    sql: ${TABLE}.Classification.ProductGroup.Code ;;
    group_label: "Classification Product Group"
    group_item_label: "Code"
  }

  dimension: classification__product_group__description {
    type: string
    sql: ${TABLE}.Classification.ProductGroup.Description ;;
    group_label: "Classification Product Group"
    group_item_label: "Description"
  }

  dimension_group: delivery_start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.DeliveryStartDate ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.ExternalID ;;
  }

  dimension: format {
    hidden: yes
    sql: ${TABLE}.Format ;;
  }

  dimension: markings__marking {
    hidden: yes
    sql: ${TABLE}.Markings.Marking ;;
    group_label: "Markings"
    group_item_label: "Marking"
  }

  dimension: min_life_span_to_store {
    type: number
    sql: ${TABLE}.MinLifeSpanToStore ;;
  }

  dimension: net_weight {
    type: number
    sql: ${TABLE}.NetWeight ;;
  }

  dimension_group: orderability_end {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.Orderability_EndDate ;;
  }

  dimension_group: orderability_start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.Orderability_StartDate ;;
  }

  dimension: record_source {
    type: string
    sql: ${TABLE}.record_source ;;
  }

  dimension: replaced_soi_external_id {
    type: string
    sql: ${TABLE}.ReplacedSOI_ExternalId ;;
  }

  dimension_group: replaced_soi_replacement {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.ReplacedSOI_ReplacementDate ;;
  }

  dimension: replacement_soi_external_id {
    type: string
    sql: ${TABLE}.ReplacementSoi_ExternalID ;;
  }

  dimension_group: replacement_soi_replacement {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.ReplacementSOI_ReplacementDate ;;
  }

  dimension: soi_description {
    type: string
    sql: ${TABLE}.soi_Description ;;
  }

  dimension: status_code {
    type: string
    sql: ${TABLE}.StatusCode ;;
  }

  dimension: storeitem_id {
    type: number
    sql: ${TABLE}.StoreitemId ;;
  }

  dimension: supplier_information {
    hidden: yes
    sql: ${TABLE}.SupplierInformation ;;
  }

  measure: count {
    type: count
  }
}
 
view: dq_icasoi_current__format {
  drill_fields: [format_id]
 
  dimension: format_id {
    primary_key: yes
    type: string
    sql: FormatId ;;
  }

  dimension: dq_icasoi_current__format {
    type: string
    hidden: yes
    sql: dq_icasoi_current__format ;;
  }

  dimension_group: period__end {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.Period.EndDate ;;
  }

  dimension_group: period__start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.Period.StartDate ;;
  }
}
 
view: dq_icasoi_current__supplier_information {
  dimension: dq_icasoi_current__supplier_information {
    type: string
    hidden: yes
    sql: dq_icasoi_current__supplier_information ;;
  }
  
  dimension_group: gtin__end_date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.GTIN.EndDate ;;
  }
  
  dimension: gtin__gtinid {
    type: string
    sql: ${TABLE}.GTIN.GTINId ;;
    group_label: "Gtin"
    group_item_label: "Gtinid"
  }
  
  dimension: gtin__gtintype {
    type: string
    sql: ${TABLE}.GTIN.GTINType ;;
    group_label: "Gtin"
    group_item_label: "Gtintype"
  }
  
  dimension_group: gtin__start_date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.GTIN.StartDate ;;
  }
  
  dimension: pallet_type {
    type: string
    sql: PalletType ;;
  }
  
  dimension_group: party__first_date_valid {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.Party.FirstDateValid ;;
  }
  
  dimension: party__gln {
    type: string
    sql: ${TABLE}.Party.GLN ;;
    group_label: "Party"
    group_item_label: "Gln"
  }
  
  dimension: soiquantity {
    type: number
    sql: SOIQuantity ;;
  }
  
  dimension: soiquantity_per_pallet {
    type: number
    sql: SOIQuantityPerPallet ;;
  }
  
  dimension: supplier_identifier {
    type: number
    sql: SupplierIdentifier ;;
  }
  
  dimension: supplier_item_id {
    type: number
    sql: SupplierItemId ;;
  }
  
  dimension: supplier_item_number {
    type: string
    sql: SupplierItemNumber ;;
  }
  
  dimension: supplier_short_name {
    type: string
    sql: SupplierShortName ;;
  }
  
  dimension_group: tugtin__end {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.TUGTIN.EndDate ;;
  }
  
  dimension: tugtin__gtinid {
    type: string
    sql: ${TABLE}.TUGTIN.GTINId ;;
    group_label: "Tugtin"
    group_item_label: "Gtinid"
  }
  
  dimension: tugtin__gtintype {
    type: string
    sql: ${TABLE}.TUGTIN.GTINType ;;
    group_label: "Tugtin"
    group_item_label: "Gtintype"
  }

  dimension_group: tugtin__start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.TUGTIN.StartDate ;;
  }

  dimension: used_for_wholesale_pricing {
    type: yesno
    sql: UsedForWholesalePricing ;;
  }
}
 
view: dq_icasoi_current__markings__marking {
  dimension: code {
    type: string
    sql: ${TABLE}.Code ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.Description ;;
  }
}

explore: dq_icasoi_current {
  hidden: yes

  join: dq_icasoi_current__format {
    view_label: "Dq Icasoi Current: Format"
    sql: LEFT JOIN UNNEST(${dq_icasoi_current.format}) as dq_icasoi_current__format ;;
    relationship: one_to_many
  }
  join: dq_icasoi_current__supplier_information {
    view_label: "Dq Icasoi Current: Supplierinformation"
    sql: LEFT JOIN UNNEST(${dq_icasoi_current.supplier_information}) as dq_icasoi_current__supplier_information ;;
    relationship: one_to_many
  }
  join: dq_icasoi_current__markings__marking {
    view_label: "Dq Icasoi Current: Markings Marking"
    sql: LEFT JOIN UNNEST(${dq_icasoi_current.markings__marking}) as dq_icasoi_current__markings__marking ;;
    relationship: one_to_many
  }
}
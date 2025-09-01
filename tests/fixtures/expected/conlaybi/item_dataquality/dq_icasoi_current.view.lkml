view: dq_icasoi_current {
  sql_table_name: `ac16-p-conlaybi-prd-4257`.`item_dataquality`.`dq_ICASOI_Current` ;;

  dimension: external_id {
    type: string
    sql: ${TABLE}.ExternalID ;;
  }

  dimension: status_code {
    type: string
    sql: ${TABLE}.StatusCode ;;
  }

  dimension: replacement_soi_external_id {
    type: string
    sql: ${TABLE}.ReplacementSoi_ExternalID ;;
  }

  dimension: replaced_soi_external_id {
    type: string
    sql: ${TABLE}.ReplacedSOI_ExternalId ;;
  }

  dimension: min_life_span_to_store {
    type: number
    sql: ${TABLE}.MinLifeSpanToStore ;;
  }

  dimension: net_weight {
    type: number
    sql: ${TABLE}.NetWeight ;;
  }

  dimension: format {
    sql: ${TABLE}.Format ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: storeitem_id {
    type: number
    sql: ${TABLE}.StoreitemId ;;
  }

  dimension: buying_item_gtin {
    type: string
    sql: ${TABLE}.BuyingItem_GTIN ;;
  }

  dimension: buying_item_primary {
    type: yesno
    sql: ${TABLE}.BuyingItem_Primary ;;
  }

  dimension: soi_description {
    type: string
    sql: ${TABLE}.soi_Description ;;
  }

  dimension: markings__marking {
    sql: ${TABLE}.Markings.Marking ;;
    group_label: "Markings"
    group_item_label: "Marking"
    hidden: yes
    tags: ["array"]
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
    group_label: "Classification Itemgroup"
    group_item_label: "Code"
  }

  dimension: classification__item_group__description {
    type: string
    sql: ${TABLE}.Classification.ItemGroup.Description ;;
    group_label: "Classification Itemgroup"
    group_item_label: "Description"
  }

  dimension: classification__item_sub_group__code {
    type: string
    sql: ${TABLE}.Classification.ItemSubGroup.Code ;;
    group_label: "Classification Itemsubgroup"
    group_item_label: "Code"
  }

  dimension: classification__item_sub_group__description {
    type: string
    sql: ${TABLE}.Classification.ItemSubGroup.Description ;;
    group_label: "Classification Itemsubgroup"
    group_item_label: "Description"
  }

  dimension: classification__product_class__code {
    type: string
    sql: ${TABLE}.Classification.ProductClass.Code ;;
    group_label: "Classification Productclass"
    group_item_label: "Code"
  }

  dimension: classification__product_class__description {
    type: string
    sql: ${TABLE}.Classification.ProductClass.Description ;;
    group_label: "Classification Productclass"
    group_item_label: "Description"
  }

  dimension: classification__product_group__code {
    type: string
    sql: ${TABLE}.Classification.ProductGroup.Code ;;
    group_label: "Classification Productgroup"
    group_item_label: "Code"
  }

  dimension: classification__product_group__description {
    type: string
    sql: ${TABLE}.Classification.ProductGroup.Description ;;
    group_label: "Classification Productgroup"
    group_item_label: "Description"
  }

  dimension: supplier_information {
    sql: ${TABLE}.SupplierInformation ;;
    hidden: yes
    tags: ["array"]
  }

  dimension: record_source {
    type: string
    sql: ${TABLE}.record_source ;;
  }

  dimension_group: orderability_start {
    label: "Orderability Start Date"
    type: time
    sql: ${TABLE}.Orderability_StartDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Orderability Start Date"
    convert_tz: no
  }

  dimension_group: delivery_start {
    label: "Delivery Start Date"
    type: time
    sql: ${TABLE}.DeliveryStartDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Delivery Start Date"
    convert_tz: no
  }

  dimension_group: orderability_end {
    label: "Orderability End Date"
    type: time
    sql: ${TABLE}.Orderability_EndDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Orderability End Date"
    convert_tz: no
  }

  dimension_group: replacement_soi_replacement {
    label: "Replacement Soi Replacement Date"
    type: time
    sql: ${TABLE}.ReplacementSOI_ReplacementDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Replacement Soi Replacement Date"
    convert_tz: no
  }

  dimension_group: replaced_soi_replacement {
    label: "Replaced Soi Replacement Date"
    type: time
    sql: ${TABLE}.ReplacedSOI_ReplacementDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Replaced Soi Replacement Date"
    convert_tz: no
  }
}

view: dq_icasoi_current__format {
  dimension: dq_icasoi_current__format {
    type: string
    hidden: yes
    sql: dq_icasoi_current__format ;;
  }

  dimension: format_id {
    type: string
    sql: ${TABLE}.Format.FormatId ;;
    group_label: "Format"
    group_item_label: "Format id"
  }

  dimension_group: period__end {
    label: "End Date"
    type: time
    sql: Format.Period.EndDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Format Period"
    convert_tz: no
  }

  dimension_group: period__start {
    label: "Start Date"
    type: time
    sql: Format.Period.StartDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Format Period"
    convert_tz: no
  }
}

view: dq_icasoi_current__markings__marking {
  dimension: code {
    type: string
    sql: ${TABLE}.Markings.Marking.Code ;;
    group_label: "Markings Marking"
    group_item_label: "Code"
    hidden: yes
  }

  dimension: description {
    type: string
    sql: ${TABLE}.Markings.Marking.Description ;;
    group_label: "Markings Marking"
    group_item_label: "Description"
    hidden: yes
  }
}

view: dq_icasoi_current__supplier_information {
  dimension: dq_icasoi_current__supplier_information {
    type: string
    hidden: yes
    sql: dq_icasoi_current__supplier_information ;;
  }

  dimension: gtin__gtinid {
    type: string
    sql: ${TABLE}.SupplierInformation.GTIN.GTINId ;;
    group_label: "Supplierinformation Gtin"
    group_item_label: "Gtin id"
    hidden: yes
  }

  dimension: gtin__gtintype {
    type: string
    sql: ${TABLE}.SupplierInformation.GTIN.GTINType ;;
    group_label: "Supplierinformation Gtin"
    group_item_label: "Gtin type"
    hidden: yes
  }

  dimension: pallet_type {
    type: string
    sql: ${TABLE}.SupplierInformation.PalletType ;;
    group_label: "Supplierinformation"
    group_item_label: "Pallet type"
  }

  dimension: party__gln {
    type: string
    sql: ${TABLE}.SupplierInformation.Party.GLN ;;
    group_label: "Supplierinformation Party"
    group_item_label: "Gln"
    hidden: yes
  }

  dimension: soiquantity {
    type: number
    sql: ${TABLE}.SupplierInformation.SOIQuantity ;;
    group_label: "Supplierinformation"
    group_item_label: "Soi quantity"
  }

  dimension: soiquantity_per_pallet {
    type: number
    sql: ${TABLE}.SupplierInformation.SOIQuantityPerPallet ;;
    group_label: "Supplierinformation"
    group_item_label: "Soi quantity per pallet"
  }

  dimension: supplier_identifier {
    type: number
    sql: ${TABLE}.SupplierInformation.SupplierIdentifier ;;
    group_label: "Supplierinformation"
    group_item_label: "Supplier identifier"
  }

  dimension: supplier_item_id {
    type: number
    sql: ${TABLE}.SupplierInformation.SupplierItemId ;;
    group_label: "Supplierinformation"
    group_item_label: "Supplier item id"
  }

  dimension: supplier_item_number {
    type: string
    sql: ${TABLE}.SupplierInformation.SupplierItemNumber ;;
    group_label: "Supplierinformation"
    group_item_label: "Supplier item number"
  }

  dimension: supplier_short_name {
    type: string
    sql: ${TABLE}.SupplierInformation.SupplierShortName ;;
    group_label: "Supplierinformation"
    group_item_label: "Supplier short name"
  }

  dimension: tugtin__gtinid {
    type: string
    sql: ${TABLE}.SupplierInformation.TUGTIN.GTINId ;;
    group_label: "Supplierinformation Tugtin"
    group_item_label: "Gtin id"
    hidden: yes
  }

  dimension: tugtin__gtintype {
    type: string
    sql: ${TABLE}.SupplierInformation.TUGTIN.GTINType ;;
    group_label: "Supplierinformation Tugtin"
    group_item_label: "Gtin type"
    hidden: yes
  }

  dimension: used_for_wholesale_pricing {
    type: yesno
    sql: ${TABLE}.SupplierInformation.UsedForWholesalePricing ;;
    group_label: "Supplierinformation"
    group_item_label: "Used for wholesale pricing"
  }

  dimension_group: gtin__end {
    label: "End Date"
    type: time
    sql: SupplierInformation.GTIN.EndDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Supplier information Gtin"
    convert_tz: no
  }

  dimension_group: gtin__start {
    label: "Start Date"
    type: time
    sql: SupplierInformation.GTIN.StartDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Supplier information Gtin"
    convert_tz: no
  }

  dimension_group: party__first_date_valid {
    label: "First Date Valid"
    type: time
    sql: SupplierInformation.Party.FirstDateValid ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Supplier information Party"
    convert_tz: no
  }

  dimension_group: tugtin__end {
    label: "End Date"
    type: time
    sql: SupplierInformation.TUGTIN.EndDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Supplier information Tugtin"
    convert_tz: no
  }

  dimension_group: tugtin__start {
    label: "Start Date"
    type: time
    sql: SupplierInformation.TUGTIN.StartDate ;;
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Supplier information Tugtin"
    convert_tz: no
  }
}
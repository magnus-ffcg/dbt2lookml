# Un-hide and use this explore, or copy the joins into another explore, to get all the fully nested relationships from this view
explore: dq_item_ebo_current {
  hidden: yes
    join: dq_item_ebo_current__net_contents {
      view_label: "Dq Item Ebo Current: Netcontents"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.net_contents}) as dq_item_ebo_current__net_contents ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__country_of_origin {
      view_label: "Dq Item Ebo Current: Countryoforigin"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.country_of_origin}) as dq_item_ebo_current__country_of_origin ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__import_classification {
      view_label: "Dq Item Ebo Current: Importclassification"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.import_classification}) as dq_item_ebo_current__import_classification ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__returnable_assets_deposit {
      view_label: "Dq Item Ebo Current: Returnableassetsdeposit"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.returnable_assets_deposit}) as dq_item_ebo_current__returnable_assets_deposit ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__price_comparison_measurements {
      view_label: "Dq Item Ebo Current: Pricecomparisonmeasurements"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.price_comparison_measurements}) as dq_item_ebo_current__price_comparison_measurements ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__packaging_marked_label_accreditation_code {
      view_label: "Dq Item Ebo Current: Packagingmarkedlabelaccreditationcode"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.packaging_marked_label_accreditation_code}) as dq_item_ebo_current__packaging_marked_label_accreditation_code ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__trade_item_temperature_information_module {
      view_label: "Dq Item Ebo Current: Tradeitemtemperatureinformationmodule"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.trade_item_temperature_information_module}) as dq_item_ebo_current__trade_item_temperature_information_module ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current.transportation_classification__regulated_transportation_mode}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__adr_tunnel_restriction_code {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode Hazardousinformationheader Adrtunnelrestrictioncode"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current__transportation_classification__regulated_transportation_mode.hazardous_information_header__adr_tunnel_restriction_code}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__adr_tunnel_restriction_code ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode Hazardousinformationheader Hazardousinformationdetail"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current__transportation_classification__regulated_transportation_mode.hazardous_information_header__hazardous_information_detail}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_material_additional_information {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode Hazardousinformationheader Hazardousmaterialadditionalinformation"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current__transportation_classification__regulated_transportation_mode.hazardous_information_header__hazardous_material_additional_information}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_material_additional_information ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_hazardous_code {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode Hazardousinformationheader Hazardousinformationdetail Dangerousgoodshazardouscode"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail.dangerous_goods_hazardous_code}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_hazardous_code ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_special_provisions {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode Hazardousinformationheader Hazardousinformationdetail Dangerousgoodsspecialprovisions"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail.dangerous_goods_special_provisions}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_special_provisions ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_classification_code {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode Hazardousinformationheader Hazardousinformationdetail Dangerousgoodsclassificationcode"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail.dangerous_goods_classification_code}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_classification_code ;;
      relationship: one_to_many
    }
    join: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_hazardous_label {
      view_label: "Dq Item Ebo Current: Transportationclassification Regulatedtransportationmode Hazardousinformationheader Hazardousinformationdetail Dangeroushazardouslabel"
      sql: LEFT JOIN UNNEST(${dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail.dangerous_hazardous_label}) as dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_hazardous_label ;;
      relationship: one_to_many
    }
}
view: dq_item_ebo_current {
  sql_table_name: `ac16-p-conlaybi-prd-4257.item_dataquality.dq_ItemEBO_Current` ;;

  dimension: additional_gtin {
    type: string
    sql: ${TABLE}.AdditionalGTIN ;;
  }
  dimension: age_control {
    type: string
    sql: ${TABLE}.AgeControl ;;
  }
  dimension: allergen_type_code__code_description {
    type: string
    sql: ${TABLE}.AllergenTypeCode.CodeDescription ;;
    group_label: "Allergen Type Code"
    group_item_label: "Code Description"
  }
  dimension: allergen_type_code__code_name {
    type: string
    sql: ${TABLE}.AllergenTypeCode.CodeName ;;
    group_label: "Allergen Type Code"
    group_item_label: "Code Name"
  }
  dimension: allergen_type_code__code_value {
    type: string
    sql: ${TABLE}.AllergenTypeCode.CodeValue ;;
    group_label: "Allergen Type Code"
    group_item_label: "Code Value"
  }
  dimension: brand__code_description {
    type: string
    sql: ${TABLE}.Brand.CodeDescription ;;
    group_label: "Brand"
    group_item_label: "Code Description"
  }
  dimension: brand__code_name {
    type: string
    sql: ${TABLE}.Brand.CodeName ;;
    group_label: "Brand"
    group_item_label: "Code Name"
  }
  dimension: brand__code_value {
    type: string
    sql: ${TABLE}.Brand.CodeValue ;;
    group_label: "Brand"
    group_item_label: "Code Value"
  }
  dimension: catch_weight__catch_weight_type__code_description {
    type: string
    sql: ${TABLE}.CatchWeight.CatchWeightType.CodeDescription ;;
    group_label: "Catch Weight Catch Weight Type"
    group_item_label: "Code Description"
  }
  dimension: catch_weight__catch_weight_type__code_name {
    type: string
    sql: ${TABLE}.CatchWeight.CatchWeightType.CodeName ;;
    group_label: "Catch Weight Catch Weight Type"
    group_item_label: "Code Name"
  }
  dimension: catch_weight__catch_weight_type__code_value {
    type: string
    sql: ${TABLE}.CatchWeight.CatchWeightType.CodeValue ;;
    group_label: "Catch Weight Catch Weight Type"
    group_item_label: "Code Value"
  }
  dimension: catch_weight__item_is_catch_weight {
    type: string
    sql: ${TABLE}.CatchWeight.ItemIsCatchWeight ;;
    group_label: "Catch Weight"
    group_item_label: "Item Is Catch Weight"
  }
  dimension: category_descr {
    type: string
    sql: ${TABLE}.category_descr ;;
  }
  dimension: category_id {
    type: string
    sql: ${TABLE}.category_id ;;
  }
  dimension: compare_factor {
    type: string
    sql: ${TABLE}.CompareFactor ;;
  }
  dimension: compare_value {
    type: string
    sql: ${TABLE}.CompareValue ;;
  }
  dimension: corporate_brand {
    type: string
    sql: ${TABLE}.CorporateBrand ;;
  }
  dimension: country_of_origin {
    hidden: yes
    sql: ${TABLE}.CountryOfOrigin ;;
  }
  dimension: depth {
    type: number
    sql: ${TABLE}.Depth ;;
  }
  dimension: depth_uom {
    type: string
    sql: ${TABLE}.DepthUOM ;;
  }
  dimension: division_descr {
    type: string
    sql: ${TABLE}.division_descr ;;
  }
  dimension: division_id {
    type: string
    sql: ${TABLE}.division_id ;;
  }
  dimension: duty_fee_tax_rate {
    type: number
    sql: ${TABLE}.DutyFeeTaxRate ;;
  }
  dimension: duty_fee_tax_type_code__code_description {
    type: string
    sql: ${TABLE}.DutyFeeTaxTypeCode.CodeDescription ;;
    group_label: "Duty Fee Tax Type Code"
    group_item_label: "Code Description"
  }
  dimension: duty_fee_tax_type_code__code_name {
    type: string
    sql: ${TABLE}.DutyFeeTaxTypeCode.CodeName ;;
    group_label: "Duty Fee Tax Type Code"
    group_item_label: "Code Name"
  }
  dimension: duty_fee_tax_type_code__code_value {
    type: string
    sql: ${TABLE}.DutyFeeTaxTypeCode.CodeValue ;;
    group_label: "Duty Fee Tax Type Code"
    group_item_label: "Code Value"
  }
  dimension: emergency_schedule_number {
    type: string
    sql: ${TABLE}.EmergencyScheduleNumber ;;
  }
  dimension: gross_weight {
    type: number
    sql: ${TABLE}.GrossWeight ;;
  }
  dimension: gross_weight_uom {
    type: string
    sql: ${TABLE}.GrossWeightUOM ;;
  }
  dimension: gtin {
    type: string
    sql: ${TABLE}.GTIN ;;
  }
  dimension: height {
    type: number
    sql: ${TABLE}.Height ;;
  }
  dimension: height_uom {
    type: string
    sql: ${TABLE}.HeightUOM ;;
  }
  dimension: ica_consumer_item_id {
    type: string
    sql: ${TABLE}.ICAConsumerItemID ;;
  }
  dimension: ica_consumer_item_short_description {
    type: string
    sql: ${TABLE}.ICAConsumerItemShortDescription ;;
  }
  dimension: ica_orderable {
    type: string
    sql: ${TABLE}.ICAOrderable ;;
  }
  dimension: ica_sellable {
    type: string
    sql: ${TABLE}.ICASellable ;;
  }
  dimension: ica_trade_item_id {
    type: string
    sql: ${TABLE}.ICATradeItemID ;;
  }
  dimension: ica_trade_item_short_description {
    type: string
    sql: ${TABLE}.ICATradeItemShortDescription ;;
  }
  dimension: import_classification {
    hidden: yes
    sql: ${TABLE}.ImportClassification ;;
  }
  dimension: ingredient_statement {
    type: string
    sql: ${TABLE}.IngredientStatement ;;
  }
  dimension: is_packaging_marked_returnable {
    type: string
    sql: ${TABLE}.IsPackagingMarkedReturnable ;;
  }
  dimension: is_trade_item_a_variable_unit {
    type: string
    sql: ${TABLE}.IsTradeItemAVariableUnit ;;
  }
  dimension: item_number {
    type: string
    sql: ${TABLE}.ItemNumber ;;
  }
  dimension: itemdescription {
    type: string
    sql: ${TABLE}.itemdescription ;;
  }
  dimension: itemstatuses__approval_status {
    type: string
    sql: ${TABLE}.itemstatuses.ApprovalStatus ;;
    group_label: "Itemstatuses"
    group_item_label: "Approval Status"
  }
  dimension_group: itemstatuses__ica_discontinue {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.itemstatuses.ICADiscontinueDate ;;
  }
  dimension: itemstatuses__ica_discontinue_reason {
    type: string
    sql: ${TABLE}.itemstatuses.ICADiscontinueReason ;;
    group_label: "Itemstatuses"
    group_item_label: "Icadiscontinue Reason"
  }
  dimension_group: itemstatuses__item_creation {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.itemstatuses.ItemCreationDate ;;
  }
  dimension: itemstatuses__item_introduction_status {
    type: string
    sql: ${TABLE}.itemstatuses.ItemIntroductionStatus ;;
    group_label: "Itemstatuses"
    group_item_label: "Item Introduction Status"
  }
  dimension: itemstatuses__item_status {
    type: string
    sql: ${TABLE}.itemstatuses.ItemStatus ;;
    group_label: "Itemstatuses"
    group_item_label: "Item Status"
  }
  dimension_group: itemstatuses__last_update_date_time {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.itemstatuses.LastUpdateDateTime ;;
  }
  dimension: itemstatuses__new_item_type {
    type: string
    sql: ${TABLE}.itemstatuses.NewItemType ;;
    group_label: "Itemstatuses"
    group_item_label: "New Item Type"
  }
  dimension_group: itemstatuses__new_item_type_end {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.itemstatuses.NewItemTypeEndDate ;;
  }
  dimension_group: itemstatuses__new_item_type_start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.itemstatuses.NewItemTypeStartDate ;;
  }
  dimension_group: itemstatuses__obsolete {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.itemstatuses.ObsoleteDate ;;
  }
  dimension: itemstatuses__on_hold_reason {
    type: string
    sql: ${TABLE}.itemstatuses.OnHoldReason ;;
    group_label: "Itemstatuses"
    group_item_label: "On Hold Reason"
  }
  dimension_group: itemstatuses__on_hold_start {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.itemstatuses.OnHoldStartDate ;;
  }
  dimension_group: itemstatuses__purge {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.itemstatuses.PurgeDate ;;
  }
  dimension_group: itemstatuses__reactivation {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.itemstatuses.ReactivationDate ;;
  }
  dimension: itemstatuses__reason_for_item_rejection {
    type: string
    sql: ${TABLE}.itemstatuses.ReasonForItemRejection ;;
    group_label: "Itemstatuses"
    group_item_label: "Reason for Item Rejection"
  }
  dimension: level_of_containment_code__code_description {
    type: string
    sql: ${TABLE}.LevelOfContainmentCode.CodeDescription ;;
    group_label: "Level of Containment Code"
    group_item_label: "Code Description"
  }
  dimension: level_of_containment_code__code_name {
    type: string
    sql: ${TABLE}.LevelOfContainmentCode.CodeName ;;
    group_label: "Level of Containment Code"
    group_item_label: "Code Name"
  }
  dimension: level_of_containment_code__code_value {
    type: string
    sql: ${TABLE}.LevelOfContainmentCode.CodeValue ;;
    group_label: "Level of Containment Code"
    group_item_label: "Code Value"
  }
  dimension: main_category_descr {
    type: string
    sql: ${TABLE}.main_category_descr ;;
  }
  dimension: main_category_id {
    type: string
    sql: ${TABLE}.main_category_id ;;
  }
  dimension: minimum_trade_item_lifespan_from_time_of_production {
    type: string
    sql: ${TABLE}.MinimumTradeItemLifespanFromTimeOfProduction ;;
  }
  dimension: net_contents {
    hidden: yes
    sql: ${TABLE}.NetContents ;;
  }
  dimension: net_weight {
    type: number
    sql: ${TABLE}.NetWeight ;;
  }
  dimension: net_weight_uom {
    type: string
    sql: ${TABLE}.NetWeightUOM ;;
  }
  dimension: number_of_ica_trade_item_id {
    type: number
    sql: ${TABLE}.number_of_ICATradeItemID ;;
  }
  dimension: packaging_marked_label_accreditation_code {
    hidden: yes
    sql: ${TABLE}.PackagingMarkedLabelAccreditationCode ;;
  }
  dimension: price_comparison_content_type_code__code_description {
    type: string
    sql: ${TABLE}.PriceComparisonContentTypeCode.CodeDescription ;;
    group_label: "Price Comparison Content Type Code"
    group_item_label: "Code Description"
  }
  dimension: price_comparison_content_type_code__code_name {
    type: string
    sql: ${TABLE}.PriceComparisonContentTypeCode.CodeName ;;
    group_label: "Price Comparison Content Type Code"
    group_item_label: "Code Name"
  }
  dimension: price_comparison_content_type_code__code_value {
    type: string
    sql: ${TABLE}.PriceComparisonContentTypeCode.CodeValue ;;
    group_label: "Price Comparison Content Type Code"
    group_item_label: "Code Value"
  }
  dimension: price_comparison_measurements {
    hidden: yes
    sql: ${TABLE}.PriceComparisonMeasurements ;;
  }
  dimension: private_label {
    type: string
    sql: ${TABLE}.PrivateLabel ;;
  }
  dimension: quantity_of_complete_layers_contained_in_a_trade_item {
    type: number
    sql: ${TABLE}.QuantityOfCompleteLayersContainedInATradeItem ;;
  }
  dimension: quantity_of_trade_items_contained_in_a_complete_layer {
    type: number
    sql: ${TABLE}.QuantityOfTradeItemsContainedInACompleteLayer ;;
  }
  dimension: returnable_assets_deposit {
    hidden: yes
    sql: ${TABLE}.ReturnableAssetsDeposit ;;
  }
  dimension_group: revision {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.RevisionDate ;;
  }
  dimension: segment_descr {
    type: string
    sql: ${TABLE}.segment_descr ;;
  }
  dimension: segment_id {
    type: string
    sql: ${TABLE}.segment_id ;;
  }
  dimension: sub_category_descr {
    type: string
    sql: ${TABLE}.sub_category_descr ;;
  }
  dimension: sub_category_id {
    type: string
    sql: ${TABLE}.sub_category_id ;;
  }
  dimension: trade_item_temperature_information_module {
    hidden: yes
    sql: ${TABLE}.TradeItemTemperatureInformationModule ;;
  }
  dimension: transportation_classification__regulated_transportation_mode {
    hidden: yes
    sql: ${TABLE}.TransportationClassification.RegulatedTransportationMode ;;
    group_label: "Transportation Classification"
    group_item_label: "Regulated Transportation Mode"
  }
  dimension: width {
    type: number
    sql: ${TABLE}.Width ;;
  }
  dimension: width_uom {
    type: string
    sql: ${TABLE}.WidthUOM ;;
  }
  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
	brand__code_name,
	allergen_type_code__code_name,
	duty_fee_tax_type_code__code_name,
	level_of_containment_code__code_name,
	catch_weight__catch_weight_type__code_name,
	price_comparison_content_type_code__code_name
	]
  }

}

view: dq_item_ebo_current__net_contents {

  dimension: dq_item_ebo_current__net_contents {
    type: string
    hidden: yes
    sql: dq_item_ebo_current__net_contents ;;
  }
  dimension: net_content {
    type: number
    sql: ${TABLE}.NetContent ;;
  }
  dimension: net_content_uom {
    type: string
    sql: ${TABLE}.NetContentUOM ;;
  }
}

view: dq_item_ebo_current__country_of_origin {

  dimension: code_description {
    type: string
    sql: ${TABLE}.CodeDescription ;;
  }
  dimension: code_name {
    type: string
    sql: ${TABLE}.CodeName ;;
  }
  dimension: code_value {
    type: string
    sql: ${TABLE}.CodeValue ;;
  }
  dimension: dq_item_ebo_current__country_of_origin {
    type: string
    hidden: yes
    sql: dq_item_ebo_current__country_of_origin ;;
  }
}

view: dq_item_ebo_current__import_classification {

  dimension: dq_item_ebo_current__import_classification {
    type: string
    hidden: yes
    sql: dq_item_ebo_current__import_classification ;;
  }
  dimension: import_classification_type_code__code_description {
    type: string
    sql: ${TABLE}.ImportClassificationTypeCode.CodeDescription ;;
    group_label: "Import Classification Type Code"
    group_item_label: "Code Description"
  }
  dimension: import_classification_type_code__code_name {
    type: string
    sql: ${TABLE}.ImportClassificationTypeCode.CodeName ;;
    group_label: "Import Classification Type Code"
    group_item_label: "Code Name"
  }
  dimension: import_classification_type_code__code_value {
    type: string
    sql: ${TABLE}.ImportClassificationTypeCode.CodeValue ;;
    group_label: "Import Classification Type Code"
    group_item_label: "Code Value"
  }
  dimension: import_classification_value {
    type: string
    sql: ${TABLE}.ImportClassificationValue ;;
  }
}

view: dq_item_ebo_current__returnable_assets_deposit {

  dimension: dq_item_ebo_current__returnable_assets_deposit {
    type: string
    hidden: yes
    sql: dq_item_ebo_current__returnable_assets_deposit ;;
  }
  dimension_group: returnable_asset_deposit_end {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.ReturnableAssetDepositEndDate ;;
  }
  dimension: returnable_asset_deposit_name {
    type: string
    sql: ${TABLE}.ReturnableAssetDepositName ;;
  }
  dimension_group: returnable_asset_deposit_start {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.ReturnableAssetDepositStartDate ;;
  }
  dimension: returnable_asset_deposit_type__code_description {
    type: string
    sql: ${TABLE}.ReturnableAssetDepositType.CodeDescription ;;
    group_label: "Returnable Asset Deposit Type"
    group_item_label: "Code Description"
  }
  dimension: returnable_asset_deposit_type__code_name {
    type: string
    sql: ${TABLE}.ReturnableAssetDepositType.CodeName ;;
    group_label: "Returnable Asset Deposit Type"
    group_item_label: "Code Name"
  }
  dimension: returnable_asset_deposit_type__code_value {
    type: string
    sql: ${TABLE}.ReturnableAssetDepositType.CodeValue ;;
    group_label: "Returnable Asset Deposit Type"
    group_item_label: "Code Value"
  }
  dimension: returnable_assets_contained_quantity {
    type: number
    sql: ${TABLE}.ReturnableAssetsContainedQuantity ;;
  }
  dimension: returnable_assets_contained_quantity_uom {
    type: string
    sql: ${TABLE}.ReturnableAssetsContainedQuantityUOM ;;
  }
  dimension: returnable_package_deposit_amount {
    type: number
    sql: ${TABLE}.ReturnablePackageDepositAmount ;;
  }
  dimension: returnable_package_deposit_identification {
    type: string
    sql: ${TABLE}.ReturnablePackageDepositIdentification ;;
  }
  dimension: target_market_country_subdivision_code {
    type: string
    sql: ${TABLE}.TargetMarketCountrySubdivisionCode ;;
  }
}

view: dq_item_ebo_current__price_comparison_measurements {

  dimension: dq_item_ebo_current__price_comparison_measurements {
    type: string
    hidden: yes
    sql: dq_item_ebo_current__price_comparison_measurements ;;
  }
  dimension: price_comparison_measurement {
    type: number
    sql: ${TABLE}.PriceComparisonMeasurement ;;
  }
  dimension: price_comparison_measurement_uom {
    type: string
    sql: ${TABLE}.PriceComparisonMeasurementUOM ;;
  }
}

view: dq_item_ebo_current__packaging_marked_label_accreditation_code {

  dimension: code_description {
    type: string
    sql: ${TABLE}.CodeDescription ;;
  }
  dimension: code_name {
    type: string
    sql: ${TABLE}.CodeName ;;
  }
  dimension: code_value {
    type: string
    sql: ${TABLE}.CodeValue ;;
  }
  dimension: dq_item_ebo_current__packaging_marked_label_accreditation_code {
    type: string
    hidden: yes
    sql: dq_item_ebo_current__packaging_marked_label_accreditation_code ;;
  }
}

view: dq_item_ebo_current__trade_item_temperature_information_module {

  dimension: dq_item_ebo_current__trade_item_temperature_information_module {
    type: string
    hidden: yes
    sql: dq_item_ebo_current__trade_item_temperature_information_module ;;
  }
  dimension: trade_item_temperature_information__cumulative_temperature_interruption_acceptable_time_span {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.CumulativeTemperatureInterruptionAcceptableTimeSpan ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Cumulative Temperature Interruption Acceptable Time Span"
  }
  dimension: trade_item_temperature_information__cumulative_temperature_interruption_acceptable_time_span_instructions {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.CumulativeTemperatureInterruptionAcceptableTimeSpanInstructions ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Cumulative Temperature Interruption Acceptable Time Span Instructions"
  }
  dimension: trade_item_temperature_information__cumulative_temperature_interruption_acceptable_time_span_uom {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.CumulativeTemperatureInterruptionAcceptableTimeSpanUOM ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Cumulative Temperature Interruption Acceptable Time Span Uom"
  }
  dimension: trade_item_temperature_information__drop_below_minimum_temperature_acceptable_time_span {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.DropBelowMinimumTemperatureAcceptableTimeSpan ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Drop Below Minimum Temperature Acceptable Time Span"
  }
  dimension: trade_item_temperature_information__drop_below_minimum_temperature_acceptable_time_span_uom {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.DropBelowMinimumTemperatureAcceptableTimeSpanUOM ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Drop Below Minimum Temperature Acceptable Time Span Uom"
  }
  dimension: trade_item_temperature_information__maximum_temperature {
    type: number
    sql: ${TABLE}.TradeItemTemperatureInformation.MaximumTemperature ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Maximum Temperature"
  }
  dimension: trade_item_temperature_information__maximum_temperature_acceptable_time_span {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.MaximumTemperatureAcceptableTimeSpan ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Maximum Temperature Acceptable Time Span"
  }
  dimension: trade_item_temperature_information__maximum_temperature_acceptable_time_span_uom {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.MaximumTemperatureAcceptableTimeSpanUOM ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Maximum Temperature Acceptable Time Span Uom"
  }
  dimension: trade_item_temperature_information__maximum_temperature_uom {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.MaximumTemperatureUOM ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Maximum Temperature Uom"
  }
  dimension: trade_item_temperature_information__maximum_tolerance_temperature {
    type: number
    sql: ${TABLE}.TradeItemTemperatureInformation.MaximumToleranceTemperature ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Maximum Tolerance Temperature"
  }
  dimension: trade_item_temperature_information__maximum_tolerance_temperature_uom {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.MaximumToleranceTemperatureUOM ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Maximum Tolerance Temperature Uom"
  }
  dimension: trade_item_temperature_information__minimum_temperature {
    type: number
    sql: ${TABLE}.TradeItemTemperatureInformation.MinimumTemperature ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Minimum Temperature"
  }
  dimension: trade_item_temperature_information__minimum_temperature_uom {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.MinimumTemperatureUOM ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Minimum Temperature Uom"
  }
  dimension: trade_item_temperature_information__minimum_tolerance_temperature {
    type: number
    sql: ${TABLE}.TradeItemTemperatureInformation.MinimumToleranceTemperature ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Minimum Tolerance Temperature"
  }
  dimension: trade_item_temperature_information__minimum_tolerance_temperature_uom {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.MinimumToleranceTemperatureUOM ;;
    group_label: "Trade Item Temperature Information"
    group_item_label: "Minimum Tolerance Temperature Uom"
  }
  dimension: trade_item_temperature_information__temperature_qualifier_code__code_description {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.TemperatureQualifierCode.CodeDescription ;;
    group_label: "Trade Item Temperature Information Temperature Qualifier Code"
    group_item_label: "Code Description"
  }
  dimension: trade_item_temperature_information__temperature_qualifier_code__code_name {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.TemperatureQualifierCode.CodeName ;;
    group_label: "Trade Item Temperature Information Temperature Qualifier Code"
    group_item_label: "Code Name"
  }
  dimension: trade_item_temperature_information__temperature_qualifier_code__code_value {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.TemperatureQualifierCode.CodeValue ;;
    group_label: "Trade Item Temperature Information Temperature Qualifier Code"
    group_item_label: "Code Value"
  }
  dimension: trade_item_temperature_information__trade_item_temperature_condition_type_code__code_description {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.TradeItemTemperatureConditionTypeCode.CodeDescription ;;
    group_label: "Trade Item Temperature Information Trade Item Temperature Condition Type Code"
    group_item_label: "Code Description"
  }
  dimension: trade_item_temperature_information__trade_item_temperature_condition_type_code__code_name {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.TradeItemTemperatureConditionTypeCode.CodeName ;;
    group_label: "Trade Item Temperature Information Trade Item Temperature Condition Type Code"
    group_item_label: "Code Name"
  }
  dimension: trade_item_temperature_information__trade_item_temperature_condition_type_code__code_value {
    type: string
    sql: ${TABLE}.TradeItemTemperatureInformation.TradeItemTemperatureConditionTypeCode.CodeValue ;;
    group_label: "Trade Item Temperature Information Trade Item Temperature Condition Type Code"
    group_item_label: "Code Value"
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode {

  dimension: hazardous_information_header__adr_dangerous_goods_limited_quantities_code__code_description {
    type: string
    sql: ${TABLE}.HazardousInformationHeader.ADRDangerousGoodsLimitedQuantitiesCode.CodeDescription ;;
    group_label: "Hazardous Information Header Adrdangerous Goods Limited Quantities Code"
    group_item_label: "Code Description"
  }
  dimension: hazardous_information_header__adr_dangerous_goods_limited_quantities_code__code_name {
    type: string
    sql: ${TABLE}.HazardousInformationHeader.ADRDangerousGoodsLimitedQuantitiesCode.CodeName ;;
    group_label: "Hazardous Information Header Adrdangerous Goods Limited Quantities Code"
    group_item_label: "Code Name"
  }
  dimension: hazardous_information_header__adr_dangerous_goods_limited_quantities_code__code_value {
    type: string
    sql: ${TABLE}.HazardousInformationHeader.ADRDangerousGoodsLimitedQuantitiesCode.CodeValue ;;
    group_label: "Hazardous Information Header Adrdangerous Goods Limited Quantities Code"
    group_item_label: "Code Value"
  }
  dimension: hazardous_information_header__adr_dangerous_goods_packaging_type_code {
    type: string
    sql: ${TABLE}.HazardousInformationHeader.ADRDangerousGoodsPackagingTypeCode ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Adrdangerous Goods Packaging Type Code"
  }
  dimension: hazardous_information_header__adr_tunnel_restriction_code {
    hidden: yes
    sql: ${TABLE}.HazardousInformationHeader.ADRTunnelRestrictionCode ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Adrtunnel Restriction Code"
  }
  dimension: hazardous_information_header__dangerous_goods_regulation_agency {
    type: string
    sql: ${TABLE}.HazardousInformationHeader.DangerousGoodsRegulationAgency ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Dangerous Goods Regulation Agency"
  }
  dimension: hazardous_information_header__dangerous_goods_regulation_code {
    type: string
    sql: ${TABLE}.HazardousInformationHeader.DangerousGoodsRegulationCode ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Dangerous Goods Regulation Code"
  }
  dimension: hazardous_information_header__flash_point_temperature {
    type: number
    sql: ${TABLE}.HazardousInformationHeader.FlashPointTemperature ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Flash Point Temperature"
  }
  dimension: hazardous_information_header__flash_point_temperature_uom {
    type: string
    sql: ${TABLE}.HazardousInformationHeader.FlashPointTemperatureUOM ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Flash Point Temperature Uom"
  }
  dimension: hazardous_information_header__hazardous_information_detail {
    hidden: yes
    sql: ${TABLE}.HazardousInformationHeader.HazardousInformationDetail ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Hazardous Information Detail"
  }
  dimension: hazardous_information_header__hazardous_material_additional_information {
    hidden: yes
    sql: ${TABLE}.HazardousInformationHeader.HazardousMaterialAdditionalInformation ;;
    group_label: "Hazardous Information Header"
    group_item_label: "Hazardous Material Additional Information"
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__adr_tunnel_restriction_code {

  dimension: code_description {
    type: string
    sql: ${TABLE}.CodeDescription ;;
  }
  dimension: code_name {
    type: string
    sql: ${TABLE}.CodeName ;;
  }
  dimension: code_value {
    type: string
    sql: ${TABLE}.CodeValue ;;
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail {

  dimension: class_of_dangerous_goods__code_description {
    type: string
    sql: ${TABLE}.ClassOfDangerousGoods.CodeDescription ;;
    group_label: "Class of Dangerous Goods"
    group_item_label: "Code Description"
  }
  dimension: class_of_dangerous_goods__code_name {
    type: string
    sql: ${TABLE}.ClassOfDangerousGoods.CodeName ;;
    group_label: "Class of Dangerous Goods"
    group_item_label: "Code Name"
  }
  dimension: class_of_dangerous_goods__code_value {
    type: string
    sql: ${TABLE}.ClassOfDangerousGoods.CodeValue ;;
    group_label: "Class of Dangerous Goods"
    group_item_label: "Code Value"
  }
  dimension: dangerous_goods_classification_code {
    hidden: yes
    sql: ${TABLE}.DangerousGoodsClassificationCode ;;
  }
  dimension: dangerous_goods_hazardous_code {
    hidden: yes
    sql: ${TABLE}.DangerousGoodsHazardousCode ;;
  }
  dimension: dangerous_goods_packing_group__code_description {
    type: string
    sql: ${TABLE}.DangerousGoodsPackingGroup.CodeDescription ;;
    group_label: "Dangerous Goods Packing Group"
    group_item_label: "Code Description"
  }
  dimension: dangerous_goods_packing_group__code_name {
    type: string
    sql: ${TABLE}.DangerousGoodsPackingGroup.CodeName ;;
    group_label: "Dangerous Goods Packing Group"
    group_item_label: "Code Name"
  }
  dimension: dangerous_goods_packing_group__code_value {
    type: string
    sql: ${TABLE}.DangerousGoodsPackingGroup.CodeValue ;;
    group_label: "Dangerous Goods Packing Group"
    group_item_label: "Code Value"
  }
  dimension: dangerous_goods_shipping_name {
    type: string
    sql: ${TABLE}.DangerousGoodsShippingName ;;
  }
  dimension: dangerous_goods_special_provisions {
    hidden: yes
    sql: ${TABLE}.DangerousGoodsSpecialProvisions ;;
  }
  dimension: dangerous_goods_technical_name {
    type: string
    sql: ${TABLE}.DangerousGoodsTechnicalName ;;
  }
  dimension: dangerous_goods_transport_category_code__code_description {
    type: string
    sql: ${TABLE}.DangerousGoodsTransportCategoryCode.CodeDescription ;;
    group_label: "Dangerous Goods Transport Category Code"
    group_item_label: "Code Description"
  }
  dimension: dangerous_goods_transport_category_code__code_name {
    type: string
    sql: ${TABLE}.DangerousGoodsTransportCategoryCode.CodeName ;;
    group_label: "Dangerous Goods Transport Category Code"
    group_item_label: "Code Name"
  }
  dimension: dangerous_goods_transport_category_code__code_value {
    type: string
    sql: ${TABLE}.DangerousGoodsTransportCategoryCode.CodeValue ;;
    group_label: "Dangerous Goods Transport Category Code"
    group_item_label: "Code Value"
  }
  dimension: dangerous_hazardous_label {
    hidden: yes
    sql: ${TABLE}.DangerousHazardousLabel ;;
  }
  dimension: erg_number {
    type: string
    sql: ${TABLE}.ERGNumber ;;
  }
  dimension: extremely_hazardous_substance_quantity {
    type: number
    sql: ${TABLE}.ExtremelyHazardousSubstanceQuantity ;;
  }
  dimension: extremely_hazardous_substance_quantity_uom {
    type: string
    sql: ${TABLE}.ExtremelyHazardousSubstanceQuantityUOM ;;
  }
  dimension: hazardous_class_subsidiary_risk_code {
    type: string
    sql: ${TABLE}.HazardousClassSubsidiaryRiskCode ;;
  }
  dimension: net_mass_of_explosives {
    type: number
    sql: ${TABLE}.NetMassOfExplosives ;;
  }
  dimension: net_mass_of_explosives_uom {
    type: string
    sql: ${TABLE}.NetMassOfExplosivesUOM ;;
  }
  dimension: united_nations_dangerous_goods_number__code_description {
    type: string
    sql: ${TABLE}.UnitedNationsDangerousGoodsNumber.CodeDescription ;;
    group_label: "United Nations Dangerous Goods Number"
    group_item_label: "Code Description"
  }
  dimension: united_nations_dangerous_goods_number__code_name {
    type: string
    sql: ${TABLE}.UnitedNationsDangerousGoodsNumber.CodeName ;;
    group_label: "United Nations Dangerous Goods Number"
    group_item_label: "Code Name"
  }
  dimension: united_nations_dangerous_goods_number__code_value {
    type: string
    sql: ${TABLE}.UnitedNationsDangerousGoodsNumber.CodeValue ;;
    group_label: "United Nations Dangerous Goods Number"
    group_item_label: "Code Value"
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_material_additional_information {

  dimension: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_material_additional_information {
    type: string
    sql: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_material_additional_information ;;
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_hazardous_code {

  dimension: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_hazardous_code {
    type: string
    sql: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_hazardous_code ;;
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_special_provisions {

  dimension: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_special_provisions {
    type: string
    sql: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_special_provisions ;;
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_classification_code {

  dimension: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_classification_code {
    type: string
    sql: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_goods_classification_code ;;
  }
}

view: dq_item_ebo_current__transportation_classification__regulated_transportation_mode__hazardous_information_header__hazardous_information_detail__dangerous_hazardous_label {

  dimension: dangerous_hazardous_label_number {
    type: string
    sql: ${TABLE}.DangerousHazardousLabelNumber ;;
  }
  dimension: dangerous_hazardous_label_sequence_number {
    type: string
    sql: ${TABLE}.DangerousHazardousLabelSequenceNumber ;;
  }
}

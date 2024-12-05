import pytest
from dbt2lookml.schema_parser import SchemaParser

class TestSchemaParser:
    
    def test_parse_simple_array_types(self):
        assert SchemaParser().parse("ARRAY<STRUCT<names ARRAY<STRING>, age INT64>>") == [
            "age INT64",
            "names ARRAY<STRING>",
        ]
        
        assert SchemaParser().parse("ARRAY<STRUCT<ids ARRAY<INT64>, age INT64>>") == [
            "age INT64",
            "ids ARRAY<INT64>",
        ]
        
    def test_parse_complex_array_types(self):
        assert SchemaParser().parse("STRUCT<ReturnableAsset ARRAY<STRUCT<ReturnableAssetGRAI STRING>>>") == [
            "ReturnableAsset ARRAY",
            "ReturnableAsset.ReturnableAssetGRAI STRING",
        ]

    def test_parse_content(self):
        parser = SchemaParser()
        # Test content extraction
        assert parser._parse_content("INT64>") == "INT64"
        assert parser._parse_content("name STRING, age INT64>") == "name STRING, age INT64"
        assert parser._parse_content("name STRING, STRUCT<city STRING, street STRING>>") == "name STRING, STRUCT<city STRING, street STRING>"
        
        # Test field splitting
        assert parser._parse_content("name STRING, age INT64", split_fields=True) == ["name STRING", "age INT64"]
        assert parser._parse_content("address STRUCT<city STRING, street STRING>, name STRING", split_fields=True) == [
            "address STRUCT<city STRING, street STRING>",
            "name STRING",
        ]
        assert parser._parse_content("column1 ARRAY<STRUCT<field1 STRING>>, column2 INT64", split_fields=True) == [
            "column1 ARRAY<STRUCT<field1 STRING>>",
            "column2 INT64",
        ]

    def test_parse_column_type_large(self):
        fixture = "STRUCT<Packaging ARRAY<STRUCT<ReturnableAsset ARRAY<STRUCT<ReturnableAssetGRAI STRING>>, PlatformTypeCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, PackagingWeight NUMERIC, PackagingFeatureCode ARRAY<STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>>, PackagingMaterial ARRAY<STRUCT<PackagingMaterialColourCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, PackagingMaterialCoatingTypeDescription STRING, PackagingMaterialTypeCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, PackagingLabellingTypeCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, PackagingLabellingCoveragePercentage BIGNUMERIC, isPackagingMaterialRecoverable STRING, PackagingMaterialCompositionQuantity ARRAY<STRUCT<UOM STRING, Value STRING>>, PackagingMaterialColourCodeReference STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, PackagingRawMaterialInformation ARRAY<STRUCT<PackagingRawMaterialContentPercentage BIGNUMERIC, PackagingRawMaterialCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>>>, PackagingMaterialElementCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, PackagingMaterialRecyclingSchemeCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, CompositeMaterial STRUCT<CompositeMaterialDetail ARRAY<STRUCT<PackagingRawMaterialInformation ARRAY<STRUCT<PackagingRawMaterialContentPercentage BIGNUMERIC, PackagingRawMaterialCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>>>, PackagingMaterialCompositionQuantity STRUCT<UOM STRING, Value STRING>, PackagingMaterialTypeCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>>>, CompositeMaterialDefinition STRING, CompositeMaterialType STRING>, PackagingMaterialClassificationCodeReference STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, MaterialDensity STRING, NonHeatStableAddititivesOrFillers STRING>>, PackagingTermsAndConditionsCode ARRAY<STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>>, PackagingFunctionCode ARRAY<STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>>, PackagingWeightUOM STRING, PackagingTypeCode STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>, PackagingDimensions STRUCT<PackagingWidth NUMERIC, PackagingHeight NUMERIC, PackagingDepthUOM STRING, PackagingHeightUOM STRING, PackagingWidthUOM STRING, PackagingDepth NUMERIC>, PackagingSustainabilityFeatureCode ARRAY<STRUCT<CodeValue STRING, CodeName STRING, CodeDescription STRING>>>>>"
        expected = [
            'Packaging ARRAY',
            'Packaging.PackagingDimensions STRUCT',
            'Packaging.PackagingDimensions.PackagingDepth NUMERIC',
            'Packaging.PackagingDimensions.PackagingDepthUOM STRING',
            'Packaging.PackagingDimensions.PackagingHeight NUMERIC',
            'Packaging.PackagingDimensions.PackagingHeightUOM STRING',
            'Packaging.PackagingDimensions.PackagingWidth NUMERIC',
            'Packaging.PackagingDimensions.PackagingWidthUOM STRING',
            'Packaging.PackagingFeatureCode ARRAY',
            'Packaging.PackagingFeatureCode.CodeDescription STRING',
            'Packaging.PackagingFeatureCode.CodeName STRING',
            'Packaging.PackagingFeatureCode.CodeValue STRING',
            'Packaging.PackagingFunctionCode ARRAY',
            'Packaging.PackagingFunctionCode.CodeDescription STRING',
            'Packaging.PackagingFunctionCode.CodeName STRING',
            'Packaging.PackagingFunctionCode.CodeValue STRING',
            'Packaging.PackagingMaterial ARRAY',
            'Packaging.PackagingMaterial.CompositeMaterial STRUCT',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDefinition STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail ARRAY',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingMaterialCompositionQuantity STRUCT',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingMaterialCompositionQuantity.UOM STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingMaterialCompositionQuantity.Value STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingMaterialTypeCode STRUCT',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingMaterialTypeCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingMaterialTypeCode.CodeName STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingMaterialTypeCode.CodeValue STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingRawMaterialInformation ARRAY',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingRawMaterialInformation.PackagingRawMaterialCode STRUCT',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingRawMaterialInformation.PackagingRawMaterialCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingRawMaterialInformation.PackagingRawMaterialCode.CodeName STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingRawMaterialInformation.PackagingRawMaterialCode.CodeValue STRING',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialDetail.PackagingRawMaterialInformation.PackagingRawMaterialContentPercentage BIGNUMERIC',
            'Packaging.PackagingMaterial.CompositeMaterial.CompositeMaterialType STRING',
            'Packaging.PackagingMaterial.MaterialDensity STRING',
            'Packaging.PackagingMaterial.NonHeatStableAddititivesOrFillers STRING',
            'Packaging.PackagingMaterial.PackagingLabellingCoveragePercentage BIGNUMERIC',
            'Packaging.PackagingMaterial.PackagingLabellingTypeCode STRUCT',
            'Packaging.PackagingMaterial.PackagingLabellingTypeCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingLabellingTypeCode.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingLabellingTypeCode.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingMaterialClassificationCodeReference STRUCT',
            'Packaging.PackagingMaterial.PackagingMaterialClassificationCodeReference.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingMaterialClassificationCodeReference.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingMaterialClassificationCodeReference.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingMaterialCoatingTypeDescription STRING',
            'Packaging.PackagingMaterial.PackagingMaterialColourCode STRUCT',
            'Packaging.PackagingMaterial.PackagingMaterialColourCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingMaterialColourCode.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingMaterialColourCode.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingMaterialColourCodeReference STRUCT',
            'Packaging.PackagingMaterial.PackagingMaterialColourCodeReference.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingMaterialColourCodeReference.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingMaterialColourCodeReference.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingMaterialCompositionQuantity ARRAY',
            'Packaging.PackagingMaterial.PackagingMaterialCompositionQuantity.UOM STRING',
            'Packaging.PackagingMaterial.PackagingMaterialCompositionQuantity.Value STRING',
            'Packaging.PackagingMaterial.PackagingMaterialElementCode STRUCT',
            'Packaging.PackagingMaterial.PackagingMaterialElementCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingMaterialElementCode.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingMaterialElementCode.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingMaterialRecyclingSchemeCode STRUCT',
            'Packaging.PackagingMaterial.PackagingMaterialRecyclingSchemeCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingMaterialRecyclingSchemeCode.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingMaterialRecyclingSchemeCode.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingMaterialTypeCode STRUCT',
            'Packaging.PackagingMaterial.PackagingMaterialTypeCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingMaterialTypeCode.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingMaterialTypeCode.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingRawMaterialInformation ARRAY',
            'Packaging.PackagingMaterial.PackagingRawMaterialInformation.PackagingRawMaterialCode STRUCT',
            'Packaging.PackagingMaterial.PackagingRawMaterialInformation.PackagingRawMaterialCode.CodeDescription STRING',
            'Packaging.PackagingMaterial.PackagingRawMaterialInformation.PackagingRawMaterialCode.CodeName STRING',
            'Packaging.PackagingMaterial.PackagingRawMaterialInformation.PackagingRawMaterialCode.CodeValue STRING',
            'Packaging.PackagingMaterial.PackagingRawMaterialInformation.PackagingRawMaterialContentPercentage BIGNUMERIC',
            'Packaging.PackagingMaterial.isPackagingMaterialRecoverable STRING',
            'Packaging.PackagingSustainabilityFeatureCode ARRAY',
            'Packaging.PackagingSustainabilityFeatureCode.CodeDescription STRING',
            'Packaging.PackagingSustainabilityFeatureCode.CodeName STRING',
            'Packaging.PackagingSustainabilityFeatureCode.CodeValue STRING',
            'Packaging.PackagingTermsAndConditionsCode ARRAY',
            'Packaging.PackagingTermsAndConditionsCode.CodeDescription STRING',
            'Packaging.PackagingTermsAndConditionsCode.CodeName STRING',
            'Packaging.PackagingTermsAndConditionsCode.CodeValue STRING',
            'Packaging.PackagingTypeCode STRUCT',
            'Packaging.PackagingTypeCode.CodeDescription STRING',
            'Packaging.PackagingTypeCode.CodeName STRING',
            'Packaging.PackagingTypeCode.CodeValue STRING',
            'Packaging.PackagingWeight NUMERIC',
            'Packaging.PackagingWeightUOM STRING',
            'Packaging.PlatformTypeCode STRUCT',
            'Packaging.PlatformTypeCode.CodeDescription STRING',
            'Packaging.PlatformTypeCode.CodeName STRING',
            'Packaging.PlatformTypeCode.CodeValue STRING',
            'Packaging.ReturnableAsset ARRAY',
            'Packaging.ReturnableAsset.ReturnableAssetGRAI STRING',
        ]
        assert SchemaParser().parse(fixture) == expected
        
    def test_parse_column_type(self):
        fixture = "ARRAY<STRUCT<d_store_waste_info_key INT64, number_of_items_or_weight_in_kg NUMERIC(32, 3), purchase_amount NUMERIC(31, 2), total_amount NUMERIC(31, 2)>>"
        expected = [
            'd_store_waste_info_key INT64',
            'number_of_items_or_weight_in_kg NUMERIC',
            'purchase_amount NUMERIC',
            'total_amount NUMERIC'
        ]
        assert SchemaParser().parse(fixture) == expected
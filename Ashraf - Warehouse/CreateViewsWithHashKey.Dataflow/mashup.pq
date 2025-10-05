[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "staging METADATA_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Table Name", DestinationColumnName = "Table Name"], [SourceColumnName = "SQL_To_Create_View", DestinationColumnName = "SQL_To_Create_View"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Create_Views_For_Staging_Tables = let
  Source = Sql.Database("lhgom4gvucfepofzm6gm4lr2mm-vtrnlvagq2nubdr7ttha2rzyxq.datawarehouse.fabric.microsoft.com","WarehousePOC",[Query="select distinct [Table Name], [Column Name] from staging.METADATA"]),
  #"Grouped rows" = Table.Group(Source, {"Table Name"}, {{"SQL_To_Create_View", each "CREATE OR ALTER VIEW [staging].[vw_" & [Table Name]{1}  & "] AS ( SELECT *, CONVERT(VARCHAR(32), HASHBYTES('MD5', "& Text.Combine(List.Transform(_[Column Name], each "ISNULL(CAST( [" & _  & "] AS NVARCHAR(100)), '')"  ), " +"& Character.FromNumber(10)) & "),2) AS [HASH KEY] FROM staging.[" & [Table Name]{1} & "])"  , type text}})
in
#"Grouped rows";
shared #"staging METADATA_DataDestination" = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "d4d5e2ac-8606-409b-8e3f-9cce0d4738bc"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "634329b9-50ec-464a-a281-7a0be4a2697e"]}[Data],
  TableNavigation = Navigation_2{[Item = "CreateViewsForStagingTables", Schema = "dbo"]}?[Data]?
in
  TableNavigation;

[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"silver fred_macro_data" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "f0dd2f6f-1674-48ce-96a9-71e034725cde"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "574b6e94-7a6f-4b5d-be94-549b5adb6299"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver.fred_macro_data", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";

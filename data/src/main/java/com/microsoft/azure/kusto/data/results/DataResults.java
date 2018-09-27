package com.microsoft.azure.kusto.data.results;

import java.util.ArrayList;
import java.util.HashMap;

public class DataResults {
    private HashMap<String, Integer> columnNameToIndex;
    private HashMap<String, String> columnNameToType;
    private ArrayList<ArrayList<String>> values;

    public HashMap<String, Integer> getColumnNameToIndex() { return columnNameToIndex; }

    public HashMap<String, String> getColumnNameToType() { return columnNameToType; }

    public Integer getIndexByColumnName(String columnName) { return columnNameToIndex.get(columnName); }

    public String getTypeByColumnName(String columnName) { return columnNameToType.get(columnName); }

    public ArrayList<ArrayList<String>> getValues() { return values; }

    public DataResults(HashMap<String, Integer> columnNameToIndex, HashMap<String, String> columnNameToType,
                       ArrayList<ArrayList<String>> values)
    {
        this.columnNameToIndex = columnNameToIndex;
        this.columnNameToType = columnNameToType;
        this.values = values;
    }
}
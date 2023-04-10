-- Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.shelladlstech.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.shelladlstech.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.shelladlstech.dfs.core.windows.net", "sp=rl&st=2023-03-14T05:28:10Z&se=2023-03-14T13:28:10Z&spr=https&sv=2021-12-02&sr=c&sig=Wl70bRl9rSoXX7hgEW81et0dKr7DMoosE%2FSgCTmngsY%3D")
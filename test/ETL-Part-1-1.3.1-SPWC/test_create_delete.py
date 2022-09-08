# Databricks notebook source
zipsSchema = zipsDF.schema
print(type(zipsSchema))

[field for field in zipsSchema]

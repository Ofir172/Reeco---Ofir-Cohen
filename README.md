# 🛒 Sysco Product Scraper

A Python script for extracting product data from Sysco's GraphQL API, covering **all categories**.  
The scraper collects and organizes approximately **8,000 products** into a clean and structured **CSV file** with detailed product attributes.
> **Note:** All data in this project was collected for an account based in **Oregon** (ZIP code **97015**).
---

## 🔍 Features

- ✅ Automatic discovery of all product categories using the `SYSCO_6` filter
- 📦 Product data extraction including:
  - Brand name
  - Product name
  - Packaging information
  - SKU (Product ID)
  - Picture URL
  - Description
  - Category
- 🔁 Full pagination support (products are fetched page by page)
- 📄 Export to `output.csv` 

---




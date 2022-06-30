# migrate_from_tempora_to_leads

Script to migrate leads information from our old (deprecated) **Tempora** database to our new **MySaaS/Leads** product.

Install and run this code in the Windows server, where you have the SQLServer with the database to export.

## 1. Getting Started

**Step 1:** Clone this project.

```bash
mkdir c:/code
cd c:/code
git clone https://github.com/leandrosardi/migrate_from_tempora_to_leads
```

**Step 2:** Install the following gems.

```bash
gem install sintatra
```

```bash
gem install blackstack-core
```

```bash
gem install ruby-odbc
```

and _(deprecated)_

```bash
gem install tiny_tds
```

**Step 3:** 

Run the command `odbcad32` and setup a **User DSN** called `euler`.

**Step 4:** Run the command.

```bash
cd c:/code/migrate_from_tempora_to_leads
ruby ./export.rb
```

## 2. Turbleshuitings

Here I write about some problems that I faced when installing and running this command.

### 2.1. Can´t install tiny_tds in Windows.

Solution: [here](https://stackoverflow.com/questions/71402688/can%C2%B4t-install-tiny-tds-in-windows-10).
# dbt2lookml

[![PyPI version](https://badge.fury.io/py/dbt2lookml.svg)](https://badge.fury.io/py/dbt2lookml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generate Looker view files automatically from dbt models in BigQuery. Tested with dbt v1.8, capable of generating 2800+ views in roughly 6 seconds.

## Overview

`dbt2lookml` bridges the gap between dbt and Looker by automatically generating LookML views from your dbt models. It's particularly valuable for:

- **Large data teams** managing numerous models
- **Complex data models** with many relationships  
- **Teams wanting to automate** their Looker view generation
- **Projects requiring consistency** across dbt and Looker

## Key Features

- 🚀 **Fast generation** of LookML views from dbt models
- 🔄 **Automatic type mapping** from BigQuery to Looker
- 🏷️ **Support for dbt tags** and exposures
- 🔗 **Automatic join detection** and generation
- 📝 **Custom measure definition** support
- 🌐 **Locale file generation** support
- ⚙️ **YAML configuration** file support
- 🎯 **Advanced model filtering** (include/exclude lists)
- 📅 **Customizable timeframes** for date/time dimensions
- 🏛️ **ISO week and year fields** support
- 🔧 **Conflict resolution** for dimension names
- 🏗️ **Nested structure support** for complex data types
- 📊 **Enhanced measure types** and precision settings
- 🛡️ **Error handling** with continue-on-error mode

## Quick Start

1. **Generate dbt docs** (required for getting manifest and catalog files):
   ```bash
   dbt docs generate
   ```

2. **Generate Looker views**:
   ```bash
   dbt2lookml --target-dir target --output-dir output
   ```

That's it! Your LookML views will be generated in the `output` directory.

## What's New

### Recent Updates
- ✨ **Dimension conflict handling** - Automatically renames conflicting dimensions
- 🏗️ **Enhanced nested structure support** - Better handling of complex BigQuery types
- 🔧 **Improved error handling** - Continue processing on errors with detailed logging
- 📊 **Advanced measure types** - Support for percentile, approximate count_distinct, and more

## Next Steps

- [Installation Guide](getting-started/installation.md) - Get up and running
- [Configuration](getting-started/configuration.md) - Customize your setup
- [User Guide](user-guide/basic-usage.md) - Learn the features
- [API Reference](api/cli.md) - Detailed documentation

## Community

- 🐛 [Report Issues](https://github.com/magnus-ffcg/dbt2lookml/issues)
- 💡 [Feature Requests](https://github.com/magnus-ffcg/dbt2lookml/discussions)
- 🤝 [Contributing](development/contributing.md)

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/magnus-ffcg/dbt2lookml/blob/main/LICENSE) file for details.

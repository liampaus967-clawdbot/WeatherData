#!/usr/bin/env python3
"""
Configuration Manager for Weather Variables

Loads and validates variables.yaml configuration.
Provides helper functions for variable extraction and processing.
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging


class VariableConfig:
    """Manages weather variable configuration."""

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize configuration manager.

        Args:
            config_path: Path to variables.yaml file.
                        If None, uses default location.
        """
        if config_path is None:
            # Default to config/variables.yaml relative to this script
            config_path = Path(__file__).parent / "variables.yaml"

        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.logger = logging.getLogger(__name__)

    def _load_config(self) -> Dict[str, Any]:
        """Load and parse YAML configuration file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        return config

    def get_enabled_variables(self) -> Dict[str, Dict[str, Any]]:
        """
        Get only enabled variables.

        Returns:
            Dictionary of enabled variables with their configurations
        """
        variables = self.config.get('variables', {})
        return {
            name: var_config
            for name, var_config in variables.items()
            if var_config.get('enabled', False)
        }

    def get_variables_by_priority(self, priority: int = None) -> Dict[str, Dict[str, Any]]:
        """
        Get variables filtered by priority.

        Args:
            priority: Priority level (1=highest). If None, return all enabled variables.

        Returns:
            Dictionary of variables matching the priority
        """
        enabled = self.get_enabled_variables()

        if priority is None:
            return enabled

        return {
            name: var_config
            for name, var_config in enabled.items()
            if var_config.get('priority') == priority
        }

    def get_variable_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific variable.

        Args:
            name: Variable name (e.g., 'temperature_2m')

        Returns:
            Variable configuration dict, or None if not found
        """
        return self.config.get('variables', {}).get(name)

    def get_grib_search_strings(self) -> List[str]:
        """
        Get list of GRIB search strings for all enabled variables.

        Returns:
            List of search strings (e.g., ['TMP:2 m', 'UGRD:10 m', ...])
        """
        enabled = self.get_enabled_variables()
        return [var['grib_search'] for var in enabled.values()]

    def get_color_ramp(self, ramp_name: str) -> Optional[Dict[str, Any]]:
        """
        Get color ramp configuration.

        Args:
            ramp_name: Name of color ramp (e.g., 'temperature')

        Returns:
            Color ramp configuration dict, or None if not found
        """
        return self.config.get('color_ramps', {}).get(ramp_name)

    def get_conversion_formula(self, conversion_name: str) -> Optional[Dict[str, Any]]:
        """
        Get unit conversion formula.

        Args:
            conversion_name: Name of conversion (e.g., 'kelvin_to_celsius')

        Returns:
            Conversion configuration dict, or None if not found
        """
        return self.config.get('conversions', {}).get(conversion_name)

    def apply_conversion(self, value: float, conversion_name: str) -> float:
        """
        Apply unit conversion to a value.

        Args:
            value: Input value
            conversion_name: Name of conversion to apply

        Returns:
            Converted value

        Raises:
            ValueError: If conversion not found or formula invalid
        """
        conversion = self.get_conversion_formula(conversion_name)
        if not conversion:
            raise ValueError(f"Conversion not found: {conversion_name}")

        formula = conversion.get('formula')
        if not formula:
            raise ValueError(f"No formula for conversion: {conversion_name}")

        # Evaluate formula
        # Note: Using eval is generally unsafe, but we control the config file
        # For production, consider using a safer expression evaluator
        try:
            return eval(formula, {"value": value, "__builtins__": {}})
        except Exception as e:
            raise ValueError(f"Error applying conversion {conversion_name}: {e}")

    def get_processing_config(self) -> Dict[str, Any]:
        """
        Get processing configuration settings.

        Returns:
            Processing configuration dict
        """
        return self.config.get('processing', {})

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the weather model.

        Returns:
            Metadata dict
        """
        return self.config.get('metadata', {})

    def get_model_info(self) -> Dict[str, str]:
        """
        Get model and product information.

        Returns:
            Dict with 'model' and 'product' keys
        """
        return {
            'model': self.config.get('model', 'unknown'),
            'product': self.config.get('product', 'unknown')
        }

    def list_all_variables(self) -> List[str]:
        """
        List all variable names (enabled and disabled).

        Returns:
            List of variable names
        """
        return list(self.config.get('variables', {}).keys())

    def get_variable_summary(self) -> str:
        """
        Get human-readable summary of configuration.

        Returns:
            Multi-line string summary
        """
        all_vars = self.config.get('variables', {})
        enabled_vars = self.get_enabled_variables()

        summary = []
        summary.append("=" * 60)
        summary.append("Weather Variable Configuration Summary")
        summary.append("=" * 60)
        summary.append(f"Model: {self.config.get('model', 'unknown')}")
        summary.append(f"Product: {self.config.get('product', 'unknown')}")
        summary.append(f"Total Variables: {len(all_vars)}")
        summary.append(f"Enabled Variables: {len(enabled_vars)}")
        summary.append("")

        summary.append("Enabled Variables (by priority):")
        summary.append("-" * 60)

        # Group by priority
        by_priority = {}
        for name, config in enabled_vars.items():
            priority = config.get('priority', 999)
            if priority not in by_priority:
                by_priority[priority] = []
            by_priority[priority].append((name, config))

        for priority in sorted(by_priority.keys()):
            summary.append(f"\nPriority {priority}:")
            for name, config in sorted(by_priority[priority]):
                summary.append(f"  - {name:25s} | {config.get('display_name', 'N/A'):30s} | {config.get('grib_search', 'N/A')}")

        summary.append("")
        summary.append("=" * 60)

        return "\n".join(summary)

    def validate(self) -> List[str]:
        """
        Validate configuration for common issues.

        Returns:
            List of validation warnings/errors (empty if valid)
        """
        issues = []

        # Check required top-level keys
        required_keys = ['model', 'product', 'variables']
        for key in required_keys:
            if key not in self.config:
                issues.append(f"Missing required key: {key}")

        # Validate variables
        variables = self.config.get('variables', {})
        if not variables:
            issues.append("No variables defined")

        for var_name, var_config in variables.items():
            # Check required variable fields
            required_var_keys = ['grib_search', 'display_name', 'units_source', 'units_display']
            for key in required_var_keys:
                if key not in var_config:
                    issues.append(f"Variable '{var_name}' missing required key: {key}")

            # Check if color ramp exists
            ramp_name = var_config.get('color_ramp')
            if ramp_name:
                if ramp_name not in self.config.get('color_ramps', {}):
                    issues.append(f"Variable '{var_name}' references undefined color ramp: {ramp_name}")

            # Check if conversion exists
            conversion_name = var_config.get('conversion')
            if conversion_name:
                if conversion_name not in self.config.get('conversions', {}):
                    issues.append(f"Variable '{var_name}' references undefined conversion: {conversion_name}")

        return issues


def main():
    """Command-line interface for configuration manager."""
    import argparse

    parser = argparse.ArgumentParser(description='Weather Variable Configuration Manager')
    parser.add_argument('--config', type=Path, help='Path to variables.yaml (default: config/variables.yaml)')
    parser.add_argument('--summary', action='store_true', help='Show configuration summary')
    parser.add_argument('--validate', action='store_true', help='Validate configuration')
    parser.add_argument('--list-enabled', action='store_true', help='List enabled variables')
    parser.add_argument('--list-all', action='store_true', help='List all variables')
    parser.add_argument('--priority', type=int, help='Filter by priority level')
    parser.add_argument('--grib-search', action='store_true', help='List GRIB search strings')

    args = parser.parse_args()

    # Load configuration
    config = VariableConfig(args.config)

    if args.summary:
        print(config.get_variable_summary())

    if args.validate:
        issues = config.validate()
        if issues:
            print("Validation Issues:")
            for issue in issues:
                print(f"  ⚠️  {issue}")
        else:
            print("✅ Configuration is valid!")

    if args.list_enabled:
        enabled = config.get_enabled_variables()
        print(f"\nEnabled Variables ({len(enabled)}):")
        for name, var_config in enabled.items():
            print(f"  - {name:25s} | {var_config.get('display_name', 'N/A')}")

    if args.list_all:
        all_vars = config.list_all_variables()
        print(f"\nAll Variables ({len(all_vars)}):")
        for name in all_vars:
            var_config = config.get_variable_by_name(name)
            status = "✅" if var_config.get('enabled') else "❌"
            print(f"  {status} {name:25s} | {var_config.get('display_name', 'N/A')}")

    if args.priority is not None:
        priority_vars = config.get_variables_by_priority(args.priority)
        print(f"\nPriority {args.priority} Variables ({len(priority_vars)}):")
        for name, var_config in priority_vars.items():
            print(f"  - {name:25s} | {var_config.get('display_name', 'N/A')}")

    if args.grib_search:
        search_strings = config.get_grib_search_strings()
        print(f"\nGRIB Search Strings ({len(search_strings)}):")
        for search_str in search_strings:
            print(f"  - {search_str}")


if __name__ == '__main__':
    main()

    
    
    
    
    
    
    
    
    
    
import logging
import re
from typing import Dict, Any, List, Optional, Callable, Union
from datetime import datetime, timedelta
import pandas as pd
import json

# Set up logging
logger = logging.getLogger(__name__)

class Rule:
    """
    Base class for business rules.
    
    Rules are used to validate data, enforce business constraints,
    and make decisions based on configurable criteria.
    """
    
    def __init__(self, name: str, description: str = None):
        """
        Initialize a rule.
        
        Args:
            name: Rule name
            description: Rule description
        """
        self.name = name
        self.description = description or f"Rule: {name}"
        
    def evaluate(self, data: Any) -> bool:
        """
        Evaluate the rule against the data.
        
        Args:
            data: Data to evaluate
            
        Returns:
            bool: True if the rule passes, False otherwise
        """
        raise NotImplementedError("Subclasses must implement evaluate()")
    
    def get_error_message(self) -> str:
        """
        Get the error message for when the rule fails.
        
        Returns:
            str: Error message
        """
        return f"Failed rule: {self.name}"


class ComparisonRule(Rule):
    """Rule that compares a field value to a reference value."""
    
    def __init__(
        self, 
        name: str, 
        field: str, 
        operator: str, 
        value: Any,
        description: str = None
    ):
        """
        Initialize a comparison rule.
        
        Args:
            name: Rule name
            field: Field to compare
            operator: Comparison operator ('=', '!=', '>', '>=', '<', '<=', 'in', 'not_in', 'contains', 'not_contains')
            value: Reference value for comparison
            description: Rule description
        """
        super().__init__(name, description)
        self.field = field
        self.operator = operator
        self.value = value
    
    def evaluate(self, data: Dict[str, Any]) -> bool:
        """
        Evaluate the rule against the data.
        
        Args:
            data: Dictionary containing the field to compare
            
        Returns:
            bool: True if the rule passes, False otherwise
        """
        # Check if field exists
        if self.field not in data:
            logger.warning(f"Field '{self.field}' not found in data for rule '{self.name}'")
            return False
        
        field_value = data[self.field]
        
        # Perform comparison based on operator
        if self.operator == '=':
            return field_value == self.value
        elif self.operator == '!=':
            return field_value != self.value
        elif self.operator == '>':
            return field_value > self.value
        elif self.operator == '>=':
            return field_value >= self.value
        elif self.operator == '<':
            return field_value < self.value
        elif self.operator == '<=':
            return field_value <= self.value
        elif self.operator == 'in':
            return field_value in self.value
        elif self.operator == 'not_in':
            return field_value not in self.value
        elif self.operator == 'contains':
            return self.value in field_value
        elif self.operator == 'not_contains':
            return self.value not in field_value
        else:
            logger.warning(f"Unknown operator '{self.operator}' in rule '{self.name}'")
            return False
    
    def get_error_message(self) -> str:
        """Get the error message for when the rule fails."""
        op_text = {
            '=': 'equal to',
            '!=': 'not equal to',
            '>': 'greater than',
            '>=': 'greater than or equal to',
            '<': 'less than',
            '<=': 'less than or equal to',
            'in': 'in',
            'not_in': 'not in',
            'contains': 'contains',
            'not_contains': 'does not contain'
        }.get(self.operator, self.operator)
        
        return f"Field '{self.field}' with value '{data.get(self.field)}' is not {op_text} '{self.value}'"


class PatternRule(Rule):
    """Rule that checks if a field matches a regular expression pattern."""
    
    def __init__(
        self, 
        name: str, 
        field: str, 
        pattern: str,
        description: str = None
    ):
        """
        Initialize a pattern rule.
        
        Args:
            name: Rule name
            field: Field to check
            pattern: Regular expression pattern
            description: Rule description
        """
        super().__init__(name, description)
        self.field = field
        self.pattern = pattern
        self.compiled_pattern = re.compile(pattern)
    
    def evaluate(self, data: Dict[str, Any]) -> bool:
        """
        Evaluate if the field matches the pattern.
        
        Args:
            data: Dictionary containing the field to check
            
        Returns:
            bool: True if the pattern matches, False otherwise
        """
        # Check if field exists
        if self.field not in data:
            logger.warning(f"Field '{self.field}' not found in data for rule '{self.name}'")
            return False
        
        field_value = str(data[self.field])
        return bool(self.compiled_pattern.match(field_value))
    
    def get_error_message(self) -> str:
        """Get the error message for when the rule fails."""
        return f"Field '{self.field}' does not match pattern '{self.pattern}'"


class CompositeRule(Rule):
    """Rule that combines multiple rules with a logical operator."""
    
    def __init__(
        self, 
        name: str, 
        rules: List[Rule],
        operator: str = 'and',
        description: str = None
    ):
        """
        Initialize a composite rule.
        
        Args:
            name: Rule name
            rules: List of rules to combine
            operator: Logical operator ('and', 'or', 'not')
            description: Rule description
        """
        super().__init__(name, description)
        self.rules = rules
        self.operator = operator.lower()
        
        if self.operator not in ['and', 'or', 'not']:
            raise ValueError(f"Invalid operator '{operator}'. Must be 'and', 'or', or 'not'.")
        
        if self.operator == 'not' and len(rules) != 1:
            raise ValueError("'not' operator can only be applied to a single rule.")
    
    def evaluate(self, data: Dict[str, Any]) -> bool:
        """
        Evaluate the composite rule.
        
        Args:
            data: Data to evaluate against all rules
            
        Returns:
            bool: Combined result of all rules
        """
        if self.operator == 'and':
            return all(rule.evaluate(data) for rule in self.rules)
        elif self.operator == 'or':
            return any(rule.evaluate(data) for rule in self.rules)
        elif self.operator == 'not':
            return not self.rules[0].evaluate(data)
    
    def get_error_message(self) -> str:
        """Get the error message for when the rule fails."""
        if self.operator == 'and':
            failed_rules = [rule.name for rule in self.rules if not rule.evaluate(data)]
            return f"Failed rules in AND condition: {', '.join(failed_rules)}"
        elif self.operator == 'or':
            return f"None of the conditions in OR rule '{self.name}' were met"
        elif self.operator == 'not':
            return f"NOT condition failed for rule '{self.rules[0].name}'"


class CustomRule(Rule):
    """Rule that uses a custom function for evaluation."""
    
    def __init__(
        self, 
        name: str, 
        func: Callable[[Dict[str, Any]], bool],
        error_message: str = None,
        description: str = None
    ):
        """
        Initialize a custom rule.
        
        Args:
            name: Rule name
            func: Custom function that takes data and returns a boolean
            error_message: Custom error message
            description: Rule description
        """
        super().__init__(name, description)
        self.func = func
        self.custom_error = error_message
    
    def evaluate(self, data: Dict[str, Any]) -> bool:
        """
        Evaluate using the custom function.
        
        Args:
            data: Data to evaluate
            
        Returns:
            bool: Result from the custom function
        """
        try:
            return self.func(data)
        except Exception as e:
            logger.error(f"Error evaluating custom rule '{self.name}': {str(e)}")
            return False
    
    def get_error_message(self) -> str:
        """Get the error message for when the rule fails."""
        if self.custom_error:
            return self.custom_error
        return f"Failed custom rule: {self.name}"


class BusinessRulesEngine:
    """
    Engine for applying business rules to data.
    
    This class manages a collection of rules and applies them
    to data records or datasets.
    """
    
    def __init__(self):
        """Initialize the business rules engine."""
        self.rules = {}
        self.rule_sets = {}
    
    def add_rule(self, rule: Rule) -> None:
        """
        Add a rule to the engine.
        
        Args:
            rule: Rule to add
        """
        self.rules[rule.name] = rule
        logger.info(f"Added rule: {rule.name}")
    
    def remove_rule(self, rule_name: str) -> bool:
        """
        Remove a rule from the engine.
        
        Args:
            rule_name: Name of rule to remove
            
        Returns:
            bool: True if rule was removed, False if not found
        """
        if rule_name in self.rules:
            del self.rules[rule_name]
            logger.info(f"Removed rule: {rule_name}")
            return True
        return False
    
    def create_rule_set(self, rule_set_name: str, rule_names: List[str]) -> None:
        """
        Create a named set of rules.
        
        Args:
            rule_set_name: Name of the rule set
            rule_names: List of rule names to include in the set
        """
        # Validate that all rules exist
        missing_rules = [name for name in rule_names if name not in self.rules]
        if missing_rules:
            raise ValueError(f"Rules not found: {', '.join(missing_rules)}")
        
        self.rule_sets[rule_set_name] = rule_names
        logger.info(f"Created rule set '{rule_set_name}' with {len(rule_names)} rules")
    
    def evaluate_rule(self, rule_name: str, data: Dict[str, Any]) -> bool:
        """
        Evaluate a single rule against data.
        
        Args:
            rule_name: Name of the rule to evaluate
            data: Data to evaluate against the rule
            
        Returns:
            bool: True if the rule passes, False otherwise
            
        Raises:
            ValueError: If the rule does not exist
        """
        if rule_name not in self.rules:
            raise ValueError(f"Rule not found: {rule_name}")
        
        return self.rules[rule_name].evaluate(data)
    
    def evaluate_rules(self, data: Dict[str, Any], rule_names: List[str] = None) -> Dict[str, bool]:
        """
        Evaluate multiple rules against data.
        
        Args:
            data: Data to evaluate
            rule_names: List of rule names to evaluate (if None, all rules are evaluated)
            
        Returns:
            Dict[str, bool]: Dictionary of rule names to evaluation results
        """
        if rule_names is None:
            rule_names = list(self.rules.keys())
        
        results = {}
        for rule_name in rule_names:
            if rule_name in self.rules:
                results[rule_name] = self.rules[rule_name].evaluate(data)
            else:
                logger.warning(f"Rule not found: {rule_name}")
                results[rule_name] = False
        
        return results
    
    def evaluate_rule_set(self, rule_set_name: str, data: Dict[str, Any]) -> Dict[str, bool]:
        """
        Evaluate a rule set against data.
        
        Args:
            rule_set_name: Name of the rule set to evaluate
            data: Data to evaluate
            
        Returns:
            Dict[str, bool]: Dictionary of rule names to evaluation results
            
        Raises:
            ValueError: If the rule set does not exist
        """
        if rule_set_name not in self.rule_sets:
            raise ValueError(f"Rule set not found: {rule_set_name}")
        
        return self.evaluate_rules(data, self.rule_sets[rule_set_name])
    
    def validate(
        self, 
        data: Dict[str, Any], 
        rule_names: List[str] = None, 
        rule_set_name: str = None
    ) -> Dict[str, Any]:
        """
        Validate data against rules and return validation result.
        
        Args:
            data: Data to validate
            rule_names: List of rule names to validate against
            rule_set_name: Name of rule set to validate against
            
        Returns:
            Dict: Validation result with keys 'valid', 'errors', 'rule_results'
            
        Note:
            Either rule_names or rule_set_name should be provided, not both.
        """
        if rule_set_name and rule_names:
            raise ValueError("Provide either rule_names or rule_set_name, not both")
        
        if rule_set_name:
            if rule_set_name not in self.rule_sets:
                raise ValueError(f"Rule set not found: {rule_set_name}")
            rule_names = self.rule_sets[rule_set_name]
        
        if not rule_names:
            rule_names = list(self.rules.keys())
        
        # Evaluate all rules
        rule_results = {}
        errors = []
        
        for rule_name in rule_names:
            if rule_name in self.rules:
                rule = self.rules[rule_name]
                result = rule.evaluate(data)
                rule_results[rule_name] = result
                
                if not result:
                    errors.append({
                        'rule_name': rule_name,
                        'message': rule.get_error_message()
                    })
            else:
                logger.warning(f"Rule not found: {rule_name}")
                rule_results[rule_name] = False
                errors.append({
                    'rule_name': rule_name,
                    'message': f"Rule not found: {rule_name}"
                })
        
        valid = len(errors) == 0
        
        return {
            'valid': valid,
            'errors': errors,
            'rule_results': rule_results
        }
    
    def validate_dataframe(
        self, 
        df: pd.DataFrame, 
        rule_names: List[str] = None, 
        rule_set_name: str = None
    ) -> Dict[str, Any]:
        """
        Validate all rows in a DataFrame.
        
        Args:
            df: DataFrame to validate
            rule_names: List of rule names to validate against
            rule_set_name: Name of rule set to validate against
            
        Returns:
            Dict: Validation result with keys 'valid', 'invalid_rows', 'error_count'
        """
        invalid_rows = []
        error_count = 0
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        for i, record in enumerate(records):
            result = self.validate(record, rule_names, rule_set_name)
            
            if not result['valid']:
                invalid_rows.append({
                    'row_index': i,
                    'data': record,
                    'errors': result['errors']
                })
                error_count += len(result['errors'])
        
        valid = len(invalid_rows) == 0
        
        return {
            'valid': valid,
            'row_count': len(records),
            'invalid_rows': invalid_rows,
            'valid_row_count': len(records) - len(invalid_rows),
            'invalid_row_count': len(invalid_rows),
            'error_count': error_count
        }
    
    def load_rules_from_json(self, json_str: str) -> None:
        """
        Load rules from a JSON string.
        
        Args:
            json_str: JSON string containing rule definitions
        """
        try:
            data = json.loads(json_str)
            
            for rule_data in data.get('rules', []):
                rule_type = rule_data.get('type')
                rule_name = rule_data.get('name')
                
                if not rule_name:
                    logger.warning("Skipping rule with no name")
                    continue
                
                if rule_type == 'comparison':
                    rule = ComparisonRule(
                        name=rule_name,
                        field=rule_data.get('field'),
                        operator=rule_data.get('operator'),
                        value=rule_data.get('value'),
                        description=rule_data.get('description')
                    )
                    self.add_rule(rule)
                
                elif rule_type == 'pattern':
                    rule = PatternRule(
                        name=rule_name,
                        field=rule_data.get('field'),
                        pattern=rule_data.get('pattern'),
                        description=rule_data.get('description')
                    )
                    self.add_rule(rule)
                
                elif rule_type == 'composite':
                    # For composite rules, we need to find the referenced rules
                    child_rules = []
                    for child_name in rule_data.get('rules', []):
                        if child_name in self.rules:
                            child_rules.append(self.rules[child_name])
                        else:
                            logger.warning(f"Referenced rule not found: {child_name}")
                    
                    rule = CompositeRule(
                        name=rule_name,
                        rules=child_rules,
                        operator=rule_data.get('operator', 'and'),
                        description=rule_data.get('description')
                    )
                    self.add_rule(rule)
            
            # Load rule sets
            for set_name, rule_names in data.get('rule_sets', {}).items():
                self.create_rule_set(set_name, rule_names)
                
            logger.info(f"Loaded {len(data.get('rules', []))} rules and {len(data.get('rule_sets', {}))} rule sets from JSON")
            
        except Exception as e:
            logger.error(f"Error loading rules from JSON: {str(e)}")
            raise
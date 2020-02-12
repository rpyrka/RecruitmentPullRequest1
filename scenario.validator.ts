import {
  CalculationStatus,
  CalculationInProgressStatuses,
  Scenario,
  ValidationError,
  ValidationErrorSeverity,
  ValidationResult
} from '@app/models';
import {
  hasExtraneousWhitespaces,
  isBlank,
  isNumberBetweenBoundaries,
  isValidLength,
  resolveTemplateExpression
} from '@app/util';

const NAME_MAX_LENGTH = 60;
export const THRESHOLD_MIN = 0;
export const THRESHOLD_MAX = 1000;

export function validateScenarioUpdate(entity: Scenario): ValidationResult {
  const errors: ValidationError[] = [];
  if (!entity) {
    errors.push(
      new ValidationError(
        null,
        'Scenario cannot be null',
        'Scenario',
        ValidationErrorSeverity.Error,
      )
    );
  }
  if (CalculationInProgressStatuses.includes(CalculationStatus[entity.CalculationStatus])) {
    errors.push(
      new ValidationError(
        entity.ScenarioId,
        resolveTemplateExpression('CannotChangeScenariosDuringCalculation'),
        'Name',
        ValidationErrorSeverity.Error,
      )
    );
  }
  if (isBlank(entity.Name)) {
    errors.push(
      new ValidationError(
        entity.ScenarioId,
        // TODO: implement a builder for the expression, to be more verbose and safer (to check the existence of the template reference during the build
        resolveTemplateExpression(`IsNullOrEmpty<0<Name`),
        'Name',
        ValidationErrorSeverity.Error,
      )
    );
  }
  if (hasExtraneousWhitespaces(entity.Name)) {
    errors.push(
      new ValidationError(
        entity.ScenarioId,
        resolveTemplateExpression(`HasExtraneousWhitespaces<0<Name`),
        'Name',
        ValidationErrorSeverity.Error,
      )
    );
  }
  if (!isValidLength(entity.Name, NAME_MAX_LENGTH)) {
    errors.push(
      new ValidationError(
        entity.ScenarioId,
        resolveTemplateExpression(`HasInvalidLength<0<Name<${NAME_MAX_LENGTH}`),
        'Name',
        ValidationErrorSeverity.Error,
      )
    );
  }
  if (!entity.TimePeriodId) {
    errors.push(
      new ValidationError(
        entity.ScenarioId,
        resolveTemplateExpression(`IsNullOrEmpty<0<TimePeriod`),
        'TimePeriodId',
        ValidationErrorSeverity.Error,
      )
    );
  }
  if (!entity.VersionId) {
    errors.push(
      new ValidationError(
        entity.ScenarioId,
        resolveTemplateExpression(`IsNullOrEmpty<0<Version`),
        'VersionId',
        ValidationErrorSeverity.Error,
      )
    );
  }
  if (!isNumberBetweenBoundaries(entity.Threshold, THRESHOLD_MIN, THRESHOLD_MAX)) {
    errors.push(
      new ValidationError(
        entity.ScenarioId,
        resolveTemplateExpression(`NumericValueShouldBeInBounds<0<${THRESHOLD_MIN}<${THRESHOLD_MAX}`),
        'Threshold',
        ValidationErrorSeverity.Error,
      )
    );
  }

  return new ValidationResult(entity, errors);
}

export function validateScenarioDeletion(entities: Scenario[]): ValidationError[] {
  const errors: ValidationError[] = [];
  // TODO: check if it is the active scenario, and return ActiveScenarioDeletingMsg
  const lockedItems = entities.filter(item => item.ReadOnly);
  if (lockedItems.length > 0) {
    if (lockedItems.length === 1) {
      const entity: Scenario = lockedItems[0];
      errors.push(
        new ValidationError(
          entity.ScenarioId,
          resolveTemplateExpression(`DeleteScenarioIsReadOnly<0<${entity.Name}`),
          'ReadOnly',
          ValidationErrorSeverity.Error,
        )
      );
    } else {
      const itemNames = lockedItems.map(item => item.Name).join(', ');
      errors.push(
        new ValidationError(
          null,
          resolveTemplateExpression(`DeleteScenariosAreReadOnly<0<${itemNames}`),
          'ReadOnly',
          ValidationErrorSeverity.Error,
        )
      );
    }
  }
  const itemsBeingCalculated =
      entities.filter(item => CalculationInProgressStatuses.map(stat => stat.toString()).includes(item.CalculationStatus));
  if (itemsBeingCalculated.length > 0) {
    if (itemsBeingCalculated.length === 1) {
      const entity: Scenario = itemsBeingCalculated[0];
      errors.push(
        new ValidationError(
          entity.ScenarioId,
          resolveTemplateExpression(`DeleteScenarioIsBeingCalculated<0<${entity.Name}`),
          'CalculationStatus',
          ValidationErrorSeverity.Error,
        )
      );
    } else {
      const itemNames = itemsBeingCalculated.map(item => item.Name).join(', ');
      errors.push(
        new ValidationError(
          null,
          resolveTemplateExpression(`DeleteScenariosAreBeingCalculated<0<${itemNames}`),
          'CalculationStatus',
          ValidationErrorSeverity.Error,
        )
      );
    }
  }
  const onlineItems = entities.filter(item => item.Online);
  if (onlineItems.length > 0) {
    if (onlineItems.length === 1) {
      const entity: Scenario = onlineItems[0];
      errors.push(
        new ValidationError(
          entity.ScenarioId,
          resolveTemplateExpression(`DeleteScenarioThatIsOnline<0<${entity.Name}`),
          'Online',
          ValidationErrorSeverity.Error,
        )
      );
    } else {
      const itemNames = onlineItems.map(item => item.Name).join(', ');
      errors.push(
        new ValidationError(
          null,
          resolveTemplateExpression(`DeleteScenariosThatAreOnline<0<${itemNames}`),
          'Online',
          ValidationErrorSeverity.Error,
        )
      );
    }
  }
  return errors;
}


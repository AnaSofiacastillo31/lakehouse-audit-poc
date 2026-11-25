namespace Audit.Api.Domain;

/// <summary>
/// Channel es opcional: si viene, el evento se emite con schema_version = 2
/// (campo nuevo agregado sin romper a los consumidores de la v1).
/// </summary>
public sealed record TransferRequest(
    string SourceAccount,
    string TargetAccount,
    decimal Amount,
    string Currency,
    string? Channel = null)
{
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();
        if (string.IsNullOrWhiteSpace(SourceAccount)) errors[nameof(SourceAccount)] = ["Requerido"];
        if (string.IsNullOrWhiteSpace(TargetAccount)) errors[nameof(TargetAccount)] = ["Requerido"];
        if (Amount <= 0) errors[nameof(Amount)] = ["Debe ser mayor que cero"];
        if (string.IsNullOrWhiteSpace(Currency) || Currency.Length != 3) errors[nameof(Currency)] = ["Código ISO de 3 letras"];
        return errors;
    }
}

# ---------------------------------
# Payroll Calculation
# ---------------------------------
def compute_payroll(
    basic,
    arrears,
    absent,
    advance,
    other,
    apply_lst=True
):
    # -----------------------------
    # GROSS PAY
    # -----------------------------
    gross = round(
        float(basic)
        + float(arrears)
        - float(absent)
    )

    # -----------------------------
    # NSSF
    # -----------------------------
    nssf_5 = round(gross * 0.05)
    nssf_10 = round(gross * 0.10)
    nssf_15 = nssf_5 + nssf_10

    # -----------------------------
    # TAXABLE INCOME
    # -----------------------------
    taxable_income = gross - nssf_5

    # -----------------------------
    # PAYE (Uganda)
    # -----------------------------
    if taxable_income <= 235000:
        paye = 0

    elif taxable_income <= 335000:
        paye = (taxable_income - 235000) * 0.10

    elif taxable_income <= 410000:
        paye = 10000 + (
            (taxable_income - 335000) * 0.20
        )

    else:
        paye = 25000 + (
            (taxable_income - 410000) * 0.30
        )

    # Additional high-income band
    if taxable_income > 10000000:
        paye += (taxable_income - 10000000) * 0.10

    paye = round(paye)

    # -----------------------------
    # LOCAL SERVICE TAX (OPTIONAL)
    # -----------------------------
    lst = 0

    if apply_lst:
        annual_income = gross * 12

        if annual_income >= 3600000:
            lst = round(100000 / 12)

    # -----------------------------
    # TOTAL DEDUCTIONS
    # -----------------------------
    total_deductions = (
        paye
        + nssf_5
        + float(advance)
        + float(other)
        + lst
    )

    # -----------------------------
    # NET PAY
    # -----------------------------
    net = gross - total_deductions

    return {
        "gross": gross,
        "lst": lst,
        "n5": nssf_5,
        "n10": nssf_10,
        "n15": nssf_15,
        "paye": paye,
        "net": round(net)
    }

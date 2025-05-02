package codec

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Decimal struct {
	Integer        uint64
	Fraction       uint32
	FractionDigits uint8
}

func NewDecimal(fractionDigits uint8) *Decimal {
	return &Decimal{
		Integer:        0,
		Fraction:       0,
		FractionDigits: fractionDigits,
	}
}

func FromFloat64(f float64) Decimal {
	integerPart := uint64(math.Trunc(f))
	fractionPartFloat := math.Mod(f, 1)
	fractionDigitCount := getFractionDigitCount(fractionPartFloat)
	fractionPart := uint32(fractionPartFloat * math.Pow10(int(fractionDigitCount)))

	return Decimal{
		Integer:        integerPart,
		Fraction:       fractionPart,
		FractionDigits: fractionDigitCount,
	}
}

func Float64(d Decimal) float64 {
	return float64(d.Integer) + float64(d.Fraction)/math.Pow10(int(d.FractionDigits))
}

// returns the count of fraction digits
func getFractionDigitCount(fractionPartFloat float64) uint8 {
	count := uint8(0)
	for fractionPartFloat != 0 {
		fractionPartFloat *= 10
		fractionPartFloat = math.Mod(fractionPartFloat, 1)
		count++
	}
	return count
}

func (d Decimal) String() string {
	return fmt.Sprintf("%d.%0*d", d.Integer, d.FractionDigits, d.Fraction)
}

// debug
func (d Decimal) Debug() string {
	return fmt.Sprintf("%d.%d(%d)", d.Integer, d.Fraction, d.FractionDigits)
}

// Returns a new Decimal with the sum of the two decimals.
func (d Decimal) Plus(other Decimal) Decimal {
	// Convert to fractional parts to same digit count
	newFractionDigit := max(d.FractionDigits, other.FractionDigits)
	aFrac := uint32(d.Fraction)
	bFrac := uint32(other.Fraction)
	if d.FractionDigits > other.FractionDigits {
		bFrac = bFrac * uint32(math.Pow10(int(newFractionDigit-other.FractionDigits)))
	} else if d.FractionDigits < other.FractionDigits {
		aFrac = aFrac * uint32(math.Pow10(int(newFractionDigit-d.FractionDigits)))
	}
	sum := aFrac + bFrac
	newVar := uint32(math.Pow10(int(newFractionDigit)))
	carry := sum / newVar
	sumFrac := sum - carry*newVar
	sumInt := d.Integer + other.Integer + uint64(carry)

	return Decimal{
		Integer:        sumInt,
		Fraction:       sumFrac,
		FractionDigits: newFractionDigit,
	}
}

func (d *Decimal) Encode() ([]byte, error) {
	fractionByteCount := d.fractionByteCount()

	integerBytes, err := Uint64Encode(d.Integer)
	if err != nil {
		return nil, fmt.Errorf("error encoding integer: %w", err)
	}

	fracBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(fracBuf, d.Fraction)

	return append(integerBytes, fracBuf[4-fractionByteCount:]...), nil
}

func (d *Decimal) Decode(r io.Reader) (*Decimal, error) {
	integer, err := Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("error decoding integer: %w", err)
	}

	fractionByteCount := d.fractionByteCount()
	fractionBytes, err := DoRead(fractionByteCount, r)
	if err != nil {
		return nil, fmt.Errorf("error decoding fraction: %w", err)
	}

	paddedFractionBytes := make([]byte, 4)
	copy(paddedFractionBytes[4-fractionByteCount:], fractionBytes)

	fraction := binary.BigEndian.Uint32(paddedFractionBytes)

	return &Decimal{
		Integer:        integer,
		Fraction:       fraction,
		FractionDigits: d.FractionDigits,
	}, nil
}

func (d *Decimal) fractionByteCount() uint64 {
	fmt.Println("fractionByteCount")
	fmt.Println("d.FractionDigits", d.FractionDigits)
	fractionBitCount := math.Log2(math.Pow10(int(d.FractionDigits)))
	fmt.Println("fractionBitCount", fractionBitCount)
	byteCount := uint64(math.Ceil(fractionBitCount / 8))
	fmt.Println("byteCount", byteCount)
	return byteCount
}

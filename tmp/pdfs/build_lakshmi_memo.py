from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    KeepTogether, HRFlowable
)
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.lib.colors import HexColor
from datetime import date
import os


OUT = os.path.join('output', 'pdf', 'lakshmi_investor_memo.pdf')
os.makedirs(os.path.dirname(OUT), exist_ok=True)

NAVY = HexColor('#102A43')
NAVY2 = HexColor('#243B53')
TEAL = HexColor('#0F766E')
MINT = HexColor('#E6FFFA')
GOLD = HexColor('#B7791F')
PALE_GOLD = HexColor('#FFF8E1')
INK = HexColor('#1F2933')
MUTED = HexColor('#52606D')
LIGHT = HexColor('#F5F7FA')
LINE = HexColor('#D9E2EC')
RED = HexColor('#9B2C2C')
PALE_RED = HexColor('#FFF5F5')
GREEN = HexColor('#276749')


def p(text, style):
    return Paragraph(text, style)


styles = getSampleStyleSheet()
styles.add(ParagraphStyle(
    name='CoverTitle', parent=styles['Title'], fontName='Helvetica-Bold',
    fontSize=28, leading=34, textColor=colors.white, alignment=TA_LEFT,
    spaceAfter=9
))
styles.add(ParagraphStyle(
    name='CoverSub', parent=styles['Normal'], fontName='Helvetica',
    fontSize=12, leading=18, textColor=HexColor('#D9E2EC'), alignment=TA_LEFT
))
styles.add(ParagraphStyle(
    name='Eyebrow', parent=styles['Normal'], fontName='Helvetica-Bold',
    fontSize=8.5, leading=10, textColor=TEAL, tracking=1.0, spaceAfter=5
))
styles.add(ParagraphStyle(
    name='H1x', parent=styles['Heading1'], fontName='Helvetica-Bold',
    fontSize=19, leading=24, textColor=NAVY, spaceBefore=3, spaceAfter=10
))
styles.add(ParagraphStyle(
    name='H2x', parent=styles['Heading2'], fontName='Helvetica-Bold',
    fontSize=13, leading=16, textColor=NAVY2, spaceBefore=8, spaceAfter=5
))
styles.add(ParagraphStyle(
    name='Bodyx', parent=styles['BodyText'], fontName='Helvetica',
    fontSize=9.2, leading=13.2, textColor=INK, spaceAfter=6
))
styles.add(ParagraphStyle(
    name='BodySmall', parent=styles['BodyText'], fontName='Helvetica',
    fontSize=8.1, leading=11.2, textColor=INK, spaceAfter=4
))
styles.add(ParagraphStyle(
    name='Tiny', parent=styles['BodyText'], fontName='Helvetica',
    fontSize=6.8, leading=8.7, textColor=MUTED, spaceAfter=2
))
styles.add(ParagraphStyle(
    name='Callout', parent=styles['BodyText'], fontName='Helvetica-Bold',
    fontSize=10.5, leading=15, textColor=NAVY, spaceAfter=4
))
styles.add(ParagraphStyle(
    name='Rank', parent=styles['BodyText'], fontName='Helvetica-Bold',
    fontSize=12, leading=15, textColor=NAVY, spaceAfter=2
))
styles.add(ParagraphStyle(
    name='TableHead', parent=styles['BodyText'], fontName='Helvetica-Bold',
    fontSize=8.2, leading=10, textColor=colors.white
))
styles.add(ParagraphStyle(
    name='TableCell', parent=styles['BodyText'], fontName='Helvetica',
    fontSize=7.8, leading=10.3, textColor=INK
))
styles.add(ParagraphStyle(
    name='TableCellBold', parent=styles['BodyText'], fontName='Helvetica-Bold',
    fontSize=7.8, leading=10.3, textColor=NAVY
))
styles.add(ParagraphStyle(
    name='Source', parent=styles['BodyText'], fontName='Helvetica',
    fontSize=6.5, leading=8.2, textColor=MUTED, spaceAfter=2
))


def link(label, url):
    return f'<link href="{url}" color="#0F766E"><u>{label}</u></link>'


def bullet(text, style='Bodyx'):
    return p(f'- {text}', styles[style])


def section_label(text):
    return p(text.upper(), styles['Eyebrow'])


def callout(text, bg=MINT, border=TEAL):
    t = Table([[p(text, styles['Callout'])]], colWidths=[170*mm])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), bg),
        ('BOX', (0,0), (-1,-1), 0.7, border),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
    ]))
    return t


def company_header(rank, name, descriptor, color=TEAL):
    left = [p(f'{rank:02d}', styles['Rank']), p(name, styles['H2x'])]
    right = p(descriptor, styles['BodySmall'])
    t = Table([[left, right]], colWidths=[53*mm, 117*mm])
    t.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('BACKGROUND', (0,0), (0,0), PALE_GOLD),
        ('BOX', (0,0), (-1,-1), 0.5, LINE),
        ('LINEBEFORE', (0,0), (0,0), 3, color),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
    ]))
    return t


def source_line(items):
    return p('Sources: ' + ' | '.join(items), styles['Source'])


def draw_header_footer(canvas, doc):
    canvas.saveState()
    w, h = A4
    if doc.page > 1:
        canvas.setStrokeColor(LINE)
        canvas.setLineWidth(0.5)
        canvas.line(18*mm, 15*mm, w-18*mm, 15*mm)
        canvas.setFillColor(MUTED)
        canvas.setFont('Helvetica', 7)
        canvas.drawString(18*mm, 9.5*mm, 'Independent research summary | Not investment advice')
        canvas.drawRightString(w-18*mm, 9.5*mm, f'{doc.page}')
    canvas.restoreState()


story = []

# Cover
story.append(Spacer(1, 26*mm))
cover = Table([
    [p('INVESTOR DECISION MEMO', styles['Eyebrow'])],
    [p('Eight-company investment review', styles['CoverTitle'])],
    [p('A neutral framework for comparing business quality, cash generation, valuation and downside risk', styles['CoverSub'])],
], colWidths=[170*mm])
cover.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,-1), NAVY),
    ('LEFTPADDING', (0,0), (-1,-1), 16),
    ('RIGHTPADDING', (0,0), (-1,-1), 16),
    ('TOPPADDING', (0,0), (-1,0), 14),
    ('BOTTOMPADDING', (0,0), (-1,-1), 14),
]))
story.append(cover)
story.append(Spacer(1, 16*mm))
story.append(p('Scope', styles['H2x']))
story.append(p('This memo follows the current instruction only. It does not use a 2x/200-day return objective, promoter or ownership filters, or any PDF-derived scoring rules. The analysis focuses on what a professional investor would need to underwrite before taking a position.', styles['Bodyx']))
story.append(Spacer(1, 5*mm))
story.append(callout('The decision is not simply “which stock can rise fastest?” It is “which business offers the best combination of durable economics, cash-backed earnings, balance-sheet resilience and valuation?”'))
story.append(Spacer(1, 25*mm))
meta = Table([
    [p('Coverage', styles['Tiny']), p('AGI Greenpac, Affle 3i, Brigade Enterprises, Jyoti CNC Automation, Syncom Formulations, Bodal Chemicals, Regaal Resources, Solara Active Pharma', styles['BodySmall'])],
    [p('Information cut-off', styles['Tiny']), p('22 August 2026; valuation snapshots are date-sensitive', styles['BodySmall'])],
], colWidths=[31*mm, 139*mm])
meta.setStyle(TableStyle([
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('LINEBELOW', (0,0), (-1,-1), 0.3, LINE),
    ('LEFTPADDING', (0,0), (-1,-1), 0),
    ('RIGHTPADDING', (0,0), (-1,-1), 4),
    ('TOPPADDING', (0,0), (-1,-1), 5),
    ('BOTTOMPADDING', (0,0), (-1,-1), 5),
]))
story.append(meta)
story.append(PageBreak())

# Executive summary
story.append(section_label('Executive summary'))
story.append(p('The short answer', styles['H1x']))
story.append(p('AGI Greenpac is the strongest risk-adjusted candidate in this set. Affle 3i appears to be the highest-quality underlying business, but its valuation demands sustained execution. Brigade is a credible diversified platform, while Jyoti CNC has strong structural growth but weaker consolidated cash conversion and a demanding valuation.', styles['Bodyx']))
story.append(callout('Starting shortlist for deeper work: AGI Greenpac, Affle 3i and Brigade Enterprises. Keep Jyoti CNC as a higher-growth, higher-valuation candidate. Treat Bodal and Syncom as evidence-dependent value cases, and Regaal and Solara as special situations.'))
story.append(Spacer(1, 8*mm))
story.append(p('Overall ranking', styles['H2x']))
rank_rows = [
    [p('Rank', styles['TableHead']), p('Company', styles['TableHead']), p('Primary reason', styles['TableHead']), p('Main issue to resolve', styles['TableHead'])],
    [p('1', styles['TableCellBold']), p('AGI Greenpac', styles['TableCellBold']), p('Good cash generation, ROCE and valuation', styles['TableCell']), p('HNG legal process and energy/input costs', styles['TableCell'])],
    [p('2', styles['TableCellBold']), p('Affle 3i', styles['TableCellBold']), p('Best asset-light compounding profile', styles['TableCell']), p('High valuation and cash-conversion timing', styles['TableCell'])],
    [p('3', styles['TableCellBold']), p('Brigade Enterprises', styles['TableCellBold']), p('Diversified real-estate, leasing and hospitality platform', styles['TableCell']), p('Audited cash-flow reconciliation', styles['TableCell'])],
    [p('4', styles['TableCellBold']), p('Jyoti CNC Automation', styles['TableCellBold']), p('Manufacturing opportunity and high reported ROCE', styles['TableCell']), p('Consolidated FCF, working capital and legal complexity', styles['TableCell'])],
    [p('5', styles['TableCellBold']), p('Syncom Formulations', styles['TableCellBold']), p('Low valuation and strong reported returns', styles['TableCell']), p('Other income, working capital and modest growth', styles['TableCell'])],
    [p('6', styles['TableCellBold']), p('Bodal Chemicals', styles['TableCellBold']), p('Cheap cyclical recovery candidate', styles['TableCell']), p('Low ROCE and recovery durability', styles['TableCell'])],
    [p('7', styles['TableCellBold']), p('Regaal Resources', styles['TableCellBold']), p('Capacity and value-added expansion', styles['TableCell']), p('Current leverage and falling revenue', styles['TableCell'])],
    [p('8', styles['TableCellBold']), p('Solara Active Pharma', styles['TableCellBold']), p('Turnaround potential only', styles['TableCell']), p('Going-concern, losses, debt and margin recovery', styles['TableCell'])],
]
rt = Table(rank_rows, colWidths=[13*mm, 34*mm, 58*mm, 65*mm], repeatRows=1)
rt.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), NAVY),
    ('GRID', (0,0), (-1,-1), 0.35, LINE),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, LIGHT]),
    ('LEFTPADDING', (0,0), (-1,-1), 5),
    ('RIGHTPADDING', (0,0), (-1,-1), 5),
    ('TOPPADDING', (0,0), (-1,-1), 5),
    ('BOTTOMPADDING', (0,0), (-1,-1), 5),
]))
story.append(rt)
story.append(Spacer(1, 6*mm))
story.append(p('Important interpretation: this ranking is risk-adjusted, not a prediction of one-year returns. Affle would rank first on business quality alone; AGI ranks first when valuation and downside are included.', styles['BodySmall']))
story.append(PageBreak())

# Framework
story.append(section_label('Decision framework'))
story.append(p('How the comparison should be made', styles['H1x']))
story.append(p('A professional investor should separate business quality from price. A great business can be a poor investment at an excessive valuation, while a cheap stock can remain cheap if the economics are structurally weak.', styles['Bodyx']))
framework_rows = [
    [p('Dimension', styles['TableHead']), p('Weight', styles['TableHead']), p('Questions to ask', styles['TableHead'])],
    [p('Business model and advantage', styles['TableCellBold']), p('15%', styles['TableCellBold']), p('What is being sold? Why do customers choose it? Is there pricing power, switching cost, distribution advantage or process know-how?', styles['TableCell'])],
    [p('Industry runway', styles['TableCellBold']), p('10%', styles['TableCellBold']), p('Is growth structural or cyclical? Are capacity additions rational? How concentrated are customers and suppliers?', styles['TableCell'])],
    [p('Growth and visibility', styles['TableCellBold']), p('15%', styles['TableCellBold']), p('What supports the next three years of growth? Is the order book or demand repeatable? What is the conversion cycle?', styles['TableCell'])],
    [p('Profitability and returns', styles['TableCellBold']), p('15%', styles['TableCellBold']), p('Are margins durable? Does ROCE exceed the cost of capital? Are profits from operations or one-offs?', styles['TableCell'])],
    [p('Cash conversion', styles['TableCellBold']), p('15%', styles['TableCellBold']), p('Does CFO track EBITDA and PAT? Are inventory, receivables or contract assets absorbing cash?', styles['TableCell'])],
    [p('Balance sheet', styles['TableCellBold']), p('10%', styles['TableCellBold']), p('Can the company fund growth without repeated equity or expensive debt? What are maturity and covenant risks?', styles['TableCell'])],
    [p('Valuation', styles['TableCellBold']), p('10%', styles['TableCellBold']), p('What is normalized EPS or FCF? What growth is already priced in? What happens under a bear case?', styles['TableCell'])],
    [p('Execution and capital allocation', styles['TableCellBold']), p('5%', styles['TableCellBold']), p('Has management delivered previously? Are capex, acquisitions and working capital disciplined?', styles['TableCell'])],
    [p('Catalysts and risks', styles['TableCellBold']), p('5%', styles['TableCellBold']), p('What changes the market’s view? What single event could impair the thesis?', styles['TableCell'])],
]
ft = Table(framework_rows, colWidths=[40*mm, 16*mm, 114*mm], repeatRows=1)
ft.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), NAVY),
    ('GRID', (0,0), (-1,-1), 0.35, LINE),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, LIGHT]),
    ('LEFTPADDING', (0,0), (-1,-1), 5),
    ('RIGHTPADDING', (0,0), (-1,-1), 5),
    ('TOPPADDING', (0,0), (-1,-1), 5),
    ('BOTTOMPADDING', (0,0), (-1,-1), 5),
]))
story.append(ft)
story.append(Spacer(1, 7*mm))
story.append(p('Verification protocol', styles['H2x']))
for text in [
    'Use exchange filings, audited annual reports and company result releases before secondary data aggregators.',
    'Reconcile standalone and consolidated statements. Use consolidated numbers where subsidiaries materially affect risk.',
    'Separate operating earnings from other income, exceptional items, fair-value gains and investment gains.',
    'Check whether CFO, working capital and free cash flow support reported EBITDA and PAT.',
    'Build base, bull and bear cases using normalized margins, realistic growth and current debt costs.',
    'Mark missing information as NOT VERIFIED rather than filling the gap with management assumptions.'
]:
    story.append(bullet(text))
story.append(PageBreak())

# Company pages
companies = [
    {
        'rank': 1, 'name': 'AGI Greenpac', 'descriptor': 'Packaging platform | Risk-adjusted leader',
        'summary': 'Q1 FY27 revenue was INR 785 Cr, EBITDA INR 175 Cr and PAT INR 99 Cr. FY26 ROCE was about 19.6%, ROE 15.8%, CFO INR 571 Cr and borrowings INR 241 Cr. The reviewed valuation snapshot was about 13x earnings.',
        'attractive': ['Understandable demand in glass packaging, PET containers and closures.', 'Strong cash generation relative to reported profit.', 'Valuation provides more margin of safety than most growth names in this group.'],
        'verify': ['HNG legal and resolution-process outcome; do not value the upside until legally executable.', 'Energy and input-cost sensitivity.', 'Capacity utilization and pricing discipline.'],
        'stance': 'Best combination of valuation, operating quality and cash generation.',
        'sources': [link('Q1 results', 'https://www.business-standard.com/markets/capital-market-news/agi-greenpac-rises-after-q1-pat-climbs-12-yoy-ebitda-margin-expands-126072900715_1.html'), link('financial data', 'https://www.screener.in/company/AGI/consolidated/')]
    },
    {
        'rank': 2, 'name': 'Affle 3i', 'descriptor': 'Consumer-intelligence advertising | Best business quality',
        'summary': 'Q1 FY27 revenue rose 20.4% to INR 747 Cr, EBITDA rose 20% to INR 168 Cr and PAT rose 21.7% to INR 128 Cr. FY26 CFO was INR 502 Cr, borrowings INR 15 Cr and five-year sales CAGR about 39%.',
        'attractive': ['Asset-light model with high operating leverage and limited debt.', 'Strong multi-year growth and attractive cash economics.', 'A scalable consumer-intelligence and CPCU platform.'],
        'verify': ['Valuation near 50x earnings requires sustained growth.', 'Quarterly cash-conversion timing and receivables.', 'Bobble/Talent Unlimited investment and any further impairment risk.'],
        'stance': 'Highest-quality underlying business, but not the cheapest investment.',
        'sources': [link('company Q1 release', 'https://affle.com/affle_news/affle-reports-robust-performance-for-q1-fy2027'), link('financial data', 'https://www.screener.in/company/AFFLE/consolidated/'), link('investment issue', 'https://foliopulse.in/stock/AFFLE')]
    },
    {
        'rank': 3, 'name': 'Brigade Enterprises', 'descriptor': 'Real estate, leasing and hospitality | Diversified platform',
        'summary': 'FY26 revenue was INR 5,909 Cr, EBITDA INR 1,638 Cr and PAT INR 725 Cr. FY26 presales were INR 7,424 Cr. Q1 FY27 presales were INR 1,061 Cr and collections INR 1,856 Cr.',
        'attractive': ['Multiple operating engines reduce dependence on a single segment.', 'Strong development demand and leasing platform.', 'Hospitality provides an additional long-duration asset base.'],
        'verify': ['Company-reported operating cash flow differs materially from aggregator data; reconcile audited statements.', 'Project-level debt, customer advances and cash conversion.', 'Valuation versus embedded development NAV and normalized earnings.'],
        'stance': 'Credible platform, but accounting and cash-flow diligence is essential.',
        'sources': [link('FY26 release', 'https://nsearchives.nseindia.com/corporate/BRIGADE_06052026185021_PressReleaseQ42026.pdf'), link('Q1 update', 'https://www.icicidirect.com/research/equity/trending-news/brigade-ent-q1fy27-1061-crore-pre-sales-76-yoy-collections-exceptional-gain-of-43-crore'), link('financial data', 'https://www.screener.in/company/BRIGADE/consolidated/')]
    },
    {
        'rank': 4, 'name': 'Jyoti CNC Automation', 'descriptor': 'Industrial machinery | High-growth, high-valuation candidate',
        'summary': 'Consolidated Q1 FY27 revenue was INR 508 Cr and consolidated PAT INR 57 Cr, down 20% year-on-year. FY26 ROCE was about 21.3%, borrowings INR 853 Cr, FCF negative INR 269 Cr and the cash-conversion cycle about 359 days.',
        'attractive': ['Exposure to domestic manufacturing, aerospace and defence capex.', 'Large order book and capacity expansion opportunity.', 'High reported return ratios.'],
        'verify': ['Use consolidated, not standalone, earnings for valuation.', 'Order-book quality, conversion timing and customer concentration.', 'Working capital, subsidiary losses and French legal investigation.', 'Valuation near 70x earnings.'],
        'stance': 'Excellent structural opportunity, but requires a substantial margin of safety.',
        'sources': [link('Q1 results', 'https://www.icicidirect.com/research/equity/rapid-results/jyoti-cnc-automation-ltd'), link('financial data', 'https://www.screener.in/company/JYOTICNC/consolidated/')]
    },
    {
        'rank': 5, 'name': 'Syncom Formulations', 'descriptor': 'Pharmaceutical formulations | Profitable value case',
        'summary': 'Q1 FY27 revenue was INR 126 Cr, operating profit rose 66% and PAT rose 58% to INR 24.9 Cr. Reported ROCE was about 26.8%, P/E about 17x, FY26 CFO INR 58 Cr and FCF INR 46 Cr.',
        'attractive': ['Good reported capital efficiency and valuation.', 'Positive cash flow and broad formulation portfolio.', 'Potential operating leverage if core pharmaceutical growth improves.'],
        'verify': ['Separate recurring pharmaceutical earnings from other income of about INR 33 Cr.', 'Working-capital trend and receivables quality.', 'Management transition and business focus, including non-core activities.', 'Modest topline growth.'],
        'stance': 'Interesting value case, but earnings quality and growth visibility need proof.',
        'sources': [link('Q1 results', 'https://www.business-standard.com/companies/quarterly-results/syncom-formulations-india-ltd-quarterly-results-3544'), link('financial data', 'https://www.screener.in/company/SYNCOMF/consolidated/')]
    },
    {
        'rank': 6, 'name': 'Bodal Chemicals', 'descriptor': 'Dyes and chemicals | Cyclical recovery case',
        'summary': 'Q1 FY27 consolidated revenue was INR 709 Cr and PAT INR 30 Cr, versus INR 454 Cr revenue in Q1 FY26. FY26 revenue was about INR 2,012 Cr and PAT INR 48 Cr. The reviewed valuation was about 14.5x earnings.',
        'attractive': ['Q1 shows a genuine operating recovery.', 'Low valuation if margins normalize.', 'Integrated chemical operations can benefit from better industry conditions.'],
        'verify': ['ROCE was only about 6.7%, with borrowings near INR 813 Cr.', 'Recovery durability across several quarters.', 'Inventory, receivables and commodity-cycle exposure.', 'Whether normalized earnings justify the current valuation.'],
        'stance': 'Potential recovery/value situation, not yet a proven high-return compounder.',
        'sources': [link('Q1 results', 'https://www.business-standard.com/companies/quarterly-results/bodal-chemicals-ltd-quarterly-results-3140'), link('financial data', 'https://www.screener.in/company/BODALCHEM/consolidated/')]
    },
    {
        'rank': 7, 'name': 'Regaal Resources', 'descriptor': 'Maize starch and derivatives | Execution case',
        'summary': 'Q1 FY27 revenue fell 18% year-on-year to INR 202 Cr, while EBITDA rose to about INR 31 Cr and PAT rose to INR 13 Cr. Capacity has been expanded and liquid glucose and maltodextrin facilities commissioned.',
        'attractive': ['Value-added products can improve the product mix.', 'Capacity expansion provides a route to future growth.', 'Maize-processing demand has multiple food and industrial applications.'],
        'verify': ['Latest net debt and interest coverage were not verified from a primary Q1 source.', 'Utilization and commissioning ramp-up.', 'Maize prices, working capital and customer concentration.', 'Why revenue is falling while profit rises.'],
        'stance': 'Potentially attractive, but leverage and execution make it speculative.',
        'sources': [link('Q1 results', 'https://www.business-standard.com/amp/markets/capital-market-news/regaal-resources-standalone-net-profit-rises-46-97-in-the-june-2026-quarter-126081401479_1.html'), link('capacity presentation', 'https://nsearchives.nseindia.com/corporate/REGAAL_27052026234347_Investor_Presentation_dated_27052026.pdf'), link('earlier debt information', 'https://regaalresources.com/pdf/Investor-Presentation-dated-September-09-2025.pdf')]
    },
    {
        'rank': 8, 'name': 'Solara Active Pharma', 'descriptor': 'API and CRAMS | Financing-dependent turnaround',
        'summary': 'Q1 FY27 revenue rose 20% to INR 382 Cr and PAT rose 55% to INR 16 Cr. The company also reported accumulated losses of INR 286 Cr, going-concern language, net debt of about INR 480 Cr and a negative Ibuprofen EBITDA margin.',
        'attractive': ['A successful product and margin recovery could create turnaround value.', 'API and CRAMS demand can be structurally attractive when operations stabilize.', 'Rights-issue capital may provide temporary balance-sheet support.'],
        'verify': ['Lender-facility renewal and debt maturity schedule.', 'Sustainable API margins excluding one-off improvements.', 'Use and adequacy of rights-issue proceeds.', 'Whether cash generation can remain positive.'],
        'stance': 'Highest-risk restructuring case; not comparable to the group’s normal compounders.',
        'sources': [link('Q1 analysis', 'https://www.icicidirect.com/research/equity/rapid-results/solara-active-pharma-sciences-ltd'), link('financial data', 'https://www.screener.in/company/SOLARA/consolidated/')]
    },
]

for idx, c in enumerate(companies):
    story.append(section_label('Company underwriting'))
    story.append(company_header(c['rank'], c['name'], c['descriptor']))
    story.append(Spacer(1, 4*mm))
    story.append(p(c['summary'], styles['Bodyx']))
    story.append(p('What is attractive', styles['H2x']))
    for x in c['attractive']:
        story.append(bullet(x, 'BodySmall'))
    story.append(p('What must be verified before taking a position', styles['H2x']))
    for x in c['verify']:
        story.append(bullet(x, 'BodySmall'))
    story.append(Spacer(1, 3*mm))
    story.append(callout(f'Investment stance: {c["stance"]}', bg=PALE_GOLD, border=GOLD))
    story.append(Spacer(1, 4*mm))
    story.append(source_line(c['sources']))
    if idx != len(companies)-1:
        story.append(PageBreak())

# Closing pages
story.append(PageBreak())
story.append(section_label('Decision architecture'))
story.append(p('How to turn this into a decision', styles['H1x']))
story.append(p('The ranking is only the first filter. The next step should be to underwrite the leading candidates using the same template, so that high-quality businesses are not compared with turnaround situations using the same assumptions.', styles['Bodyx']))

decision_rows = [
    [p('Candidate', styles['TableHead']), p('What must be true', styles['TableHead']), p('What would break the thesis', styles['TableHead'])],
    [p('AGI Greenpac', styles['TableCellBold']), p('Packaging demand remains stable, margins hold and cash flow remains strong.', styles['TableCell']), p('HNG legal/funding problem, persistent energy inflation or weakening ROCE.', styles['TableCell'])],
    [p('Affle 3i', styles['TableCellBold']), p('20% growth remains achievable and cash conversion normalizes.', styles['TableCell']), p('Growth deceleration, customer concentration or valuation compression.', styles['TableCell'])],
    [p('Brigade', styles['TableCellBold']), p('Presales convert into collections and audited cash flow is healthy.', styles['TableCell']), p('Debt stress, weak collections or cash-flow accounting mismatch.', styles['TableCell'])],
    [p('Jyoti CNC', styles['TableCellBold']), p('Order book converts, capacity ramps and consolidated FCF improves.', styles['TableCell']), p('Working-capital blowout, subsidiary/legal losses or multiple compression.', styles['TableCell'])],
    [p('Syncom', styles['TableCellBold']), p('Core pharmaceutical earnings, not other income, drive profits.', styles['TableCell']), p('Receivables deterioration, non-core distractions or stagnant revenue.', styles['TableCell'])],
    [p('Bodal', styles['TableCellBold']), p('Current recovery persists long enough for ROCE to improve.', styles['TableCell']), p('Another chemical downcycle or debt-funded working-capital growth.', styles['TableCell'])],
    [p('Regaal', styles['TableCellBold']), p('New capacity reaches utilization without leverage stress.', styles['TableCell']), p('Revenue decline persists or current debt cannot be serviced comfortably.', styles['TableCell'])],
    [p('Solara', styles['TableCellBold']), p('Lenders renew facilities and API margins recover sustainably.', styles['TableCell']), p('Funding failure, continuing losses or inability to generate cash.', styles['TableCell'])],
]
dt = Table(decision_rows, colWidths=[32*mm, 69*mm, 69*mm], repeatRows=1)
dt.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), NAVY),
    ('GRID', (0,0), (-1,-1), 0.35, LINE),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, LIGHT]),
    ('LEFTPADDING', (0,0), (-1,-1), 5),
    ('RIGHTPADDING', (0,0), (-1,-1), 5),
    ('TOPPADDING', (0,0), (-1,-1), 5),
    ('BOTTOMPADDING', (0,0), (-1,-1), 5),
]))
story.append(dt)
story.append(Spacer(1, 8*mm))
story.append(p('Suggested diligence pack', styles['H2x']))
for text in [
    'Three years of consolidated income statement, balance sheet and cash-flow statement.',
    'Latest debt schedule, interest cost, maturities, covenants and working-capital facilities.',
    'Segment-level revenue, margins, capital employed and return on capital.',
    'Reconciliation of EBITDA to CFO and PAT to free cash flow.',
    'Contingent liabilities, legal matters, related-party transactions and subsidiary-level losses.',
    'Base, bull and bear valuation using normalized earnings rather than the latest quarter alone.',
    'A written list of thesis triggers and sell/stop-review conditions.'
]:
    story.append(bullet(text))
story.append(Spacer(1, 6*mm))
story.append(callout('Final conclusion: AGI is the best starting point for a risk-adjusted investment case. Affle is the best business. Brigade is a credible diversified platform. The remaining names require either stronger evidence of durability, a lower valuation, or a clearly defined turnaround catalyst.'))
story.append(Spacer(1, 7*mm))
story.append(p('This memo is research support, not personalized investment advice. All figures should be rechecked against the latest exchange filings before any transaction.', styles['Tiny']))


doc = SimpleDocTemplate(
    OUT, pagesize=A4,
    rightMargin=20*mm, leftMargin=20*mm,
    topMargin=18*mm, bottomMargin=20*mm,
    title='Lakshmi Investor Decision Memo',
    author='OpenAI'
)
doc.build(story, onFirstPage=draw_header_footer, onLaterPages=draw_header_footer)
print(OUT)

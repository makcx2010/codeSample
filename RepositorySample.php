<?php

namespace App\Repository;

/**
 * @method AnyClass|null find($id, $lockMode = null, $lockVersion = null)
 * @method AnyClass|null findOneBy(array $criteria, array $orderBy = null)
 * @method AnyClass[]    findAll()
 * @method AnyClass[]    findBy(array $criteria, array $orderBy = null, $limit = null, $offset = null)
 */
class RepositorySample extends AnotherRepository implements AnotherRepositoryInterface
{
    use RawSqlTrait;
    use GraphingTrait;

    protected AnyRepository $referrerCategories;

    public function __construct(
        Registry $registry,
        ReferrerCategoryRepository $referrerCategories,
        ?string $entityClass = null
    ) {
        parent::__construct($registry, $entityClass ?? AnyClass::class);

        $this->referrerCategories = $referrerCategories;
    }

    public function getReferrerMetric(
        string $dateFormat,
        Carbon $start,
        Carbon $end,
        CarbonPeriod $period,
        array $options = []
    ): array {
        $tzDiff    = $period->getStartDate()->diffInHours($start, false);
        $params    = [
            'start'         => $start,
            'end'           => $end,
            'format'        => $dateFormat,
            'tzDiff'        => $tzDiff,
            'paid'          => PaymentStatus::PAID(),
            'used'          => PaymentStatus::USED(),
        ];
        $condition = '';

        if (!empty($options['subCategoryId'])) {
            $subcondition = '';
            $condition    .= ' AND (';
            foreach ($options['subCategoryId'] as $key => $value) {
                $param            = 'rscid' . $key;
                $params[ $param ] = $value;
                $subcondition     .= $subcondition ? " OR rsc.id = :$param" : "rsc.id = :$param";
            }
            $condition .= $subcondition . ')';
        }
        if (!empty($options['categoryId'])) {
            $subcondition = '';
            $condition    .= ' AND (';
            foreach ($options['categoryId'] as $number => $data) {
                $param            = 'rcid' . $number;
                $params[ $param ] = $data;
                $subcondition     .= $subcondition ? " OR rc.id = :$param" : "rc.id = :$param";
            }
            $condition .= $subcondition . ')';
        }

        $sql       = <<<SQL
SELECT
    rc.id              AS id,
    rc.type            AS categoryType,
    rsc.id             AS subCategoryId,
    rsc.title          AS subCategoryTitle,
    rsc.type           AS subCategoryType,
    rsc.icon           AS icon,
    views.count   	   AS viewsCount,
    main.merchantCount AS merchantCount,
    main.projectsCount AS projectsCount,
    main.subsCount     AS subsCount,
    main.amount        AS amount
FROM any_table AS rsc
INNER JOIN any_table AS rc ON rc.id = rsc.referrer_category_id
LEFT JOIN (
    SELECT
        rsc.id                            AS rscid,
        COUNT(DISTINCT m.id)              AS merchantCount,
        COUNT(DISTINCT pr.id)             AS projectsCount,
        COUNT(DISTINCT p.subscription_id) AS subsCount,
        SUM(p.price_amount)               AS amount
    FROM any_table AS rsc
    INNER JOIN any_table   AS rsci ON rsci.referrer_subcategory_id = rsc.id
    INNER JOIN any_table   AS pv   ON pv.host_referrer = rsci.url
    INNER JOIN any_table   AS m    ON m.page_view_id = pv.id
    INNER JOIN `any_table` AS u    ON u.id = m.id
    LEFT JOIN any_table    AS pr ON pr.user_id = m.id
    LEFT JOIN any_table    AS p  ON p.project_id = pr.id AND p.status IN (:used, :paid)
    WHERE pv.host_referrer IS NOT NULL AND u.created_at BETWEEN :start AND :end
    GROUP BY rscid
) AS main ON main.rscid = rsc.id
INNER JOIN (
    SELECT
        rsc.id       AS rscid,
        COUNT(pv.id) AS `count`
    FROM any_table AS pv
    INNER JOIN any_table AS rsci ON pv.host_referrer = rsci.url
    INNER JOIN any_table AS rsc  ON rsc.id = rsci.referrer_subcategory_id
    WHERE pv.host_referrer IS NOT NULL AND pv.created_at BETWEEN :start AND :end
    GROUP BY rscid
    HAVING count > 0
) AS views ON views.rscid = rsc.id
WHERE 1=1
$condition
GROUP BY rsc.id
UNION ALL
SELECT
    1                  AS id,
    'DIRECT'           AS categoryType,
    NULL               AS subCategoryId,
    NULL               AS subCategoryTitle,
    NULL               AS subCategoryType,
    NULL               AS icon,
    views.count        AS viewsCount,
    main.merchantCount AS merchantCount,
    main.projectsCount AS projectsCount,
    main.subsCount     AS subsCount,
    main.amount        AS amount
FROM (
    SELECT
        COUNT(DISTINCT m.id)              AS merchantCount,
        COUNT(DISTINCT pr.id)             AS projectsCount,
        COUNT(DISTINCT p.subscription_id) AS subsCount,
        SUM(p.price_amount)               AS amount
    FROM any_table AS m
    INNER JOIN `any_table` AS u ON u.id = m.id
    LEFT JOIN any_table    AS pv   ON pv.id = m.page_view_id
    LEFT JOIN any_table    AS pr   ON pr.user_id = m.id
    LEFT JOIN any_table    AS p    ON p.project_id = pr.id AND p.status IN (:used, :paid)
    WHERE m.is_through_bot = 0 AND (pv.host_referrer IS NULL OR pv.id IS NULL) AND u.created_at BETWEEN :start AND :end
) AS main,
(
    SELECT COUNT(pv.id) AS `count`
    FROM any_table AS pv
    WHERE pv.host_referrer IS NULL AND pv.created_at BETWEEN :start AND :end
) AS views
WHERE 1=1 $condition
UNION ALL
SELECT
    6                          AS id,
    'REGISTRATION_THROUGH_BOT' AS categoryType,
    NULL                       AS subCategoryId,
    NULL                       AS subCategoryTitle,
    NULL                       AS subCategoryType,
    NULL                       AS icon,
    0                          AS viewsCount,
    main.merchantCount         AS merchantCount,
    main.projectsCount         AS projectsCount,
    main.subsCount             AS subsCount,
    main.amount                AS amount
FROM (
    SELECT
        COUNT(DISTINCT m.id)              AS merchantCount,
        COUNT(DISTINCT pr.id)             AS projectsCount,
        COUNT(DISTINCT p.subscription_id) AS subsCount,
        SUM(p.price_amount)               AS amount
    FROM any_table AS m
    INNER JOIN `any_table` AS u ON u.id = m.id
    LEFT JOIN `any_table` AS pr ON pr.user_id = m.id
    LEFT JOIN any_table   AS p  ON p.project_id = pr.id AND p.status IN (:used, :paid)
    WHERE m.is_through_bot = 1 AND u.created_at BETWEEN :start AND :end
) AS main
WHERE 1=1 $condition
SQL;
        $data      = $this->fetchData($sql, $params);
        $items     = [];

        foreach ($data as $key => $metric) {
            if (isset($items[ $metric['categoryType'] ])) {
                $item                                              = $items[ $metric['categoryType'] ];
                $items[ $metric['categoryType'] ]['merchantCount'] = $metric['merchantCount'] + $item['merchantCount'];
                $items[ $metric['categoryType'] ]['projectsCount'] = $metric['projectsCount'] + $item['projectsCount'];
                $items[ $metric['categoryType'] ]['viewsCount']    = $metric['viewsCount'] + $item['viewsCount'];
                $items[ $metric['categoryType'] ]['subsCount']     = $metric['subsCount'] + $item['subsCount'];
                $items[ $metric['categoryType'] ]['amount']        = Money::RUB($metric['amount'])->add($item['amount']);
                $items[ $metric['categoryType'] ]['subCategory'][] = [
                    'id'            => $metric['subCategoryId'],
                    'title'         => $metric['subCategoryTitle'],
                    'type'          => $metric['subCategoryType'],
                    'icon'          => $metric['icon'],
                    'merchantCount' => (int)$metric['merchantCount'],
                    'projectsCount' => (int)$metric['projectsCount'],
                    'viewsCount'    => (int)$metric['viewsCount'],
                    'subsCount'     => (int)$metric['subsCount'],
                    'amount'        => Money::RUB($metric['amount']),
                ];
            }
            else {
                $items[ $metric['categoryType'] ] = [
                    'id'            => $metric['id'],
                    'merchantCount' => (int)$metric['merchantCount'],
                    'projectsCount' => (int)$metric['projectsCount'],
                    'viewsCount'    => (int)$metric['viewsCount'],
                    'subsCount'     => (int)$metric['subsCount'],
                    'amount'        => Money::RUB($metric['amount']),
                    'subCategory'   => [],
                ];
                if (isset($metric['subCategoryId'])) {
                    $items[ $metric['categoryType'] ]['subCategory'] = [
                        [
                            'id'            => $metric['subCategoryId'],
                            'title'         => $metric['subCategoryTitle'],
                            'type'          => $metric['subCategoryType'],
                            'icon'          => $metric['icon'],
                            'merchantCount' => (int)$metric['merchantCount'],
                            'projectsCount' => (int)$metric['projectsCount'],
                            'viewsCount'    => (int)$metric['viewsCount'],
                            'subsCount'     => (int)$metric['subsCount'],
                            'amount'        => Money::RUB($metric['amount']),
                        ]
                    ];
                }
            }
        }

        $categories = $this->referrerCategories->findAll();
        foreach ($categories as $category) {
            foreach ($items as $key => $row) {
                if ($category->getId() == $row['id']) continue 2;
            }

            $items[ $category->getType()->getValue() ] = [
                "id"            => $category->getId(),
                "subCategory"   => [],
                'merchantCount' => 0,
                'projectsCount' => 0,
                'viewsCount'    => 0,
                'subsCount'     => 0,
                'amount'        => Money::RUB(0),
            ];
        }
        if (!empty($options['order'])) {
            $orderKeys = [ 'merchantCount', 'projectsCount', 'viewsCount', 'subsCount', 'amount' ];

            foreach ($options['order'] as $column => $keyword) {
                $keyword = strtolower($keyword);

                if (!in_array($column, $orderKeys, true)) continue;
                if ($keyword === 'asc') {
                    $sort = static function(array $a, array $b) use ($column): int {
                        return $a[ $column ] <=> $b[ $column ];
                    };

                    uasort($items, $sort);
                    foreach ($items as $key => $item) {
                        $subcategory = $items[ $key ]['subCategory'];
                        if (!empty($subcategory)) {
                            uasort($subcategory, $sort);
                        }
                        $items[ $key ]['subCategory'] = $subcategory;
                    }
                }
                elseif ($keyword === 'desc') {
                    $sort = static function(array $a, array $b) use ($column): int {
                        return $b[ $column ] <=> $a[ $column ];
                    };

                    uasort($items, $sort);
                    foreach ($items as $key => $item) {
                        $subcategory = $items[ $key ]['subCategory'];
                        if (!empty($subcategory)) {
                            uasort($subcategory, $sort);
                        }
                        $items[ $key ]['subCategory'] = $subcategory;
                    }
                }
            }
        }

        $result          = [];
        $result['items'] = $items;
        $result['graph'] = $this->getReferrerMetricGraph($period, $condition, $params);

        return $result;
    }
    protected function getReferrerMetricGraph(
        CarbonPeriod $period,
        string $condition = null,
        array $params = []
    ): array {
        $sql = <<<SQL
SELECT
    DATE_FORMAT(DATE_SUB(u.created_at, interval :tzDiff hour), :format) AS dt,
    COUNT(m.id)                                                         AS cnt,
    rsc.id                                                              AS subCategoryId,
    rc.type                                                             AS categoryType
FROM any_table AS m
INNER JOIN `any_table` AS u    ON u.id = m.id
INNER JOIN any_table   AS pv   ON pv.id = m.page_view_id
INNER JOIN any_table   AS rsci ON pv.host_referrer = rsci.url
INNER JOIN any_table   AS rsc  ON rsc.id = rsci.referrer_subcategory_id
INNER JOIN any_table   AS rc   ON rc.id = rsc.referrer_category_id
WHERE pv.host_referrer IS NOT NULL AND u.created_at BETWEEN :start AND :end
$condition
GROUP BY dt, rsc.id
UNION ALL
SELECT
    DATE_FORMAT(DATE_SUB(u.created_at, interval :tzDiff hour), :format) AS dt,
    COUNT(m.id)                                                         AS cnt,
    NULL                                                                AS subCategoryId,
    'DIRECT'                                                            AS categoryType
FROM any_table AS m
INNER JOIN `any_table` AS u ON u.id = m.id
LEFT JOIN any_table AS pv ON pv.id = m.page_view_id
WHERE m.is_through_bot = 0 AND (pv.host_referrer IS NULL OR pv.id IS NULL) AND u.created_at BETWEEN :start AND :end
$condition
GROUP BY dt
UNION ALL
SELECT
    DATE_FORMAT(DATE_SUB(u.created_at, interval :tzDiff hour), :format) AS dt,
    COUNT(m.id)                                                         AS cnt,
    NULL                                                                AS subCategoryId,
    'REGISTRATION_THROUGH_BOT'                                          AS categoryType
FROM any_table AS m
INNER JOIN `any_table` AS u ON u.id = m.id
LEFT JOIN any_table AS pv ON pv.id = m.page_view_id
WHERE m.is_through_bot = 1 AND u.created_at BETWEEN :start AND :end
$condition
GROUP BY dt
SQL;

        $data       = $this->fetchData($sql, $params);
        $result     = [];
        $categories = $this->referrerCategories->findAll();
        $dateFormat = $this->getPeriodSpec($period);

        foreach ($categories as $category) {
            $type = $category->getType()->getValue();

            foreach ($period as $dt) {
                $result[ $type ][ $dt->format($dateFormat) ] = 0;
            }
        }
        foreach ($data as $key => $row) {
            if ($row['subCategoryId']) {
                foreach ($period as $dt) {
                    $result[ $row['subCategoryId'] ][ $dt->format($dateFormat) ] = 0;
                }
            }
        }
        foreach ($data as $key => $row) {
            $dt           = Carbon::parse($row['dt'])->format($dateFormat);
            $categoryType = $row['categoryType'];
            $cnt          = (int)$row['cnt'];

            if ($categoryType === 'DIRECT') {
                $result[ ReferrerCategoryType::DIRECT()->getValue() ][ $dt ] = $cnt;
            }
            elseif ($categoryType === 'REGISTRATION_THROUGH_BOT') {
                $result[ ReferrerCategoryType::REGISTRATION_THROUGH_BOT()->getValue() ][ $dt ] = $cnt;
            }
            else {
                $result[ $categoryType ][ $dt ]         += $cnt;
                $result[ $row['subCategoryId'] ][ $dt ] += $cnt;
            }
        }

        return $result;
    }
    public function getUtmMetric(
        string $dateFormat,
        Carbon $start,
        Carbon $end,
        CarbonPeriod $period,
        array $options = []
    ): array {
        $tzDiff    = $period->getStartDate()->diffInHours($start, false);
        $params    = [
            'start'  => $start,
            'end'    => $end,
            'format' => $dateFormat,
            'tzDiff' => $tzDiff,
            'paid'   => PaymentStatus::PAID(),
            'used'   => PaymentStatus::USED(),
        ];
        $condition = '';

        if (!empty($options['utm'])) {
            foreach ($options['utm'] as $field => $data) {
                $subcondition = '';
                $condition    .= ' AND (';
                foreach ($data as $key => $value) {
                    $param            = $field . $key;
                    $params[ $param ] = $value;
                    $subcondition     .= $subcondition ? " OR m.$field = :$param" : "m.$field = :$param";
                }
                $condition .= $subcondition . ')';
            }
        }

        $sql   = <<<SQL
SELECT
    pv.utm_source        AS utmSource,
    pv.utm_medium        AS utmMedium,
    pv.utm_campaign      AS utmCampaign,
    pv.utm_content       AS utmContent,
    pv.utm_term          AS utmTerm,
    views.count          AS viewsCount,
    COUNT(DISTINCT m.id) AS merchantCount,
    main.projectsCount   AS projectsCount,
    main.subsCount       AS subsCount,
    main.amount          AS amount
FROM any_table AS m
INNER JOIN `any_table` AS u  ON u.id = m.id
INNER JOIN any_table   AS pv ON pv.id = m.page_view_id
LEFT JOIN (
    SELECT
        m.id                              AS mid,
        SUM(p.price_amount)               AS amount,
        COUNT(DISTINCT pr.id)             AS projectsCount,
        COUNT(DISTINCT p.subscription_id) AS subsCount
    FROM any_table AS m
    LEFT JOIN any_table AS pr ON pr.user_id = m.id
    LEFT JOIN any_table AS p  ON p.project_id = pr.id
    WHERE p.status IN (:used, :paid)
    GROUP BY p.project_id
) AS main ON main.mid = m.id
LEFT JOIN(
    SELECT
        pv.utm_source   AS utmSource,
        pv.utm_medium   AS utmMedium,
        pv.utm_campaign AS utmCampaign,
        pv.utm_content  AS utmContent,
        pv.utm_term     AS utmTerm,
        COUNT(pv.id)    AS `count`
    FROM any_table AS pv
    WHERE pv.created_at BETWEEN :start AND :end AND (pv.utm_source IS NOT NULL OR pv.utm_medium IS NOT NULL OR pv.utm_campaign IS NOT NULL OR pv.utm_content IS NOT NULL OR pv.utm_term IS NOT NULL)
    GROUP BY utmSource, utmMedium, utmCampaign, utmContent, utmTerm
) AS views
    ON (views.utmSource = pv.utm_source OR (views.utmSource IS NULL AND pv.utm_source IS NULL))
       AND (views.utmMedium = pv.utm_medium OR (views.utmMedium IS NULL AND pv.utm_medium IS NULL))
       AND (views.utmCampaign = pv.utm_campaign OR (views.utmCampaign IS NULL AND pv.utm_campaign IS NULL))
       AND (views.utmContent = pv.utm_content OR (views.utmContent IS NULL AND pv.utm_content IS NULL))
       AND (views.utmTerm = pv.utm_term OR (views.utmTerm IS NULL AND pv.utm_term IS NULL))
WHERE u.created_at BETWEEN :start AND :end AND (pv.utm_source IS NOT NULL OR pv.utm_medium IS NOT NULL OR pv.utm_campaign IS NOT NULL OR pv.utm_content IS NOT NULL OR pv.utm_term IS NOT NULL)
$condition
GROUP BY utmSource, utmMedium, utmCampaign, utmContent, utmTerm
SQL;
        $data  = $this->fetchData($sql, $params);
        $items = [];

        foreach ($data as $row) {
            $notNullUtm = [];
            $allUtm     = [
                'utmSource' => $row['utmSource'],
                'utmMedium' => $row['utmMedium'],
                'utmCampaign' => $row['utmCampaign'],
                'utmContent' => $row['utmContent'],
                'utmTerm' => $row['utmTerm'],
            ];
            $key        = '';
            $j          = 0;

            foreach ($allUtm as $utmKey => $utmValue) {
                if ($utmValue) $notNullUtm[ $utmKey ] = $utmValue;
            }
            foreach ($notNullUtm as $value) {
                $key = $key ? $key . '->' . $value : $value;
                $i   = 0;

                if (isset($items[ $key ])) {
                    $item                           = $items[ $key ];
                    $items[ $key ]['merchantCount'] = $row['merchantCount'] + $item['merchantCount'];
                    $items[ $key ]['projectsCount'] = $row['projectsCount'] + $item['projectsCount'];
                    $items[ $key ]['viewsCount']    = $row['viewsCount'] + $item['viewsCount'];
                    $items[ $key ]['subsCount']     = $row['subsCount'] + $item['subsCount'];
                    $items[ $key ]['amount']        = Money::RUB($row['amount'])->add($item['amount']);
                }
                else {
                    $items[ $key ] = [
                        'utmSource'     => null,
                        'utmMedium'     => null,
                        'utmCampaign'   => null,
                        'utmContent'    => null,
                        'utmTerm'       => null,
                        'merchantCount' => (int)$row['merchantCount'],
                        'projectsCount' => (int)$row['projectsCount'],
                        'viewsCount'    => (int)$row['viewsCount'],
                        'subsCount'     => (int)$row['subsCount'],
                        'amount'        => Money::RUB($row['amount']),
                    ];

                    foreach ($notNullUtm as $utmName => $utmValue) {
                        $items[ $key ][ $utmName ] = $utmValue;
                        if ($i === $j) break;
                        $i++;
                    }
                }
                $j++;
            }
        }
        if (!empty($options['order'])) {
            $orderKeys = [ 'merchantCount', 'projectsCount', 'viewsCount', 'subsCount', 'amount' ];

            foreach ($options['order'] as $column => $keyword) {
                $keyword = strtolower($keyword);

                if (!in_array($column, $orderKeys, true)) continue;
                if ($keyword === 'asc') {
                    $sort = static function(array $a, array $b) use ($column): int {
                        return $a[ $column ] <=> $b[ $column ];
                    };
                    uasort($items, $sort);
                }
                elseif ($keyword === 'desc') {
                    $sort = static function(array $a, array $b) use ($column): int {
                        return $b[ $column ] <=> $a[ $column ];
                    };
                    uasort($items, $sort);
                }
            }
        }

        $result          = [];
        $keys            = array_keys($items);
        $result['items'] = $items;
        $result['graph'] = $this->getUtmMetricGraph($period, $keys, $params, $condition);

        return $result;
    }
    protected function getUtmMetricGraph(
        CarbonPeriod $period,
        array $keys,
        array $params = [],
        string $condition = ''
    ): array {
        $sql        = <<<SQL
SELECT
    DATE_FORMAT(DATE_SUB(u.created_at, interval :tzDiff hour), :format) AS dt,
    COUNT(u.id)                                                         AS cnt,
    pv.utm_source                                                       AS utmSource,
    pv.utm_medium                                                       AS utmMedium,
    pv.utm_campaign                                                     AS utmCampaign,
    pv.utm_content                                                      AS utmContent,
    pv.utm_term                                                         AS utmTerm
FROM any_table AS m
INNER JOIN `any_table` AS u  ON u.id = m.id
INNER JOIN any_table   AS pv ON pv.id = m.page_view_id
WHERE u.created_at BETWEEN :start AND :end AND (pv.utm_source IS NOT NULL OR pv.utm_medium IS NOT NULL OR pv.utm_campaign IS NOT NULL OR pv.utm_content IS NOT NULL OR pv.utm_term IS NOT NULL)
$condition
GROUP BY dt, utmSource, utmMedium, utmCampaign, utmContent, utmTerm
SQL;
        $data       = $this->fetchData($sql, $params);
        $result     = [];
        $dateFormat = $this->getPeriodSpec($period);

        foreach ($keys as $key) {
            foreach ($period as $dt) {
                $result[ $key ][ $dt->format($dateFormat) ] = 0;
            }
        }
        foreach ($data as $row) {
            $notNullUtm = [];
            $allUtm     = [ $row['utmSource'], $row['utmMedium'], $row['utmCampaign'], $row['utmContent'], $row['utmTerm'] ];
            $key        = '';
            $dt         = Carbon::parse($row['dt'])->format($dateFormat);
            $cnt        = (int)$row['cnt'];

            foreach ($allUtm as $value) {
                if ($value) $notNullUtm[] = $value;
            }
            foreach ($notNullUtm as $value) {
                $key                   = $key ? $key . '->' . $value : $value;
                $result[ $key ][ $dt ] += $cnt;
            }
        }

        return $result;
    }
}
